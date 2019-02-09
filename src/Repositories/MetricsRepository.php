<?php

namespace Laravel\Horizon\Repositories;

use Carbon\Carbon;
use Illuminate\Contracts\Redis\Factory;
use Laravel\Horizon\Lock;
use Laravel\Horizon\LuaScripts;

class MetricsRepository implements \Laravel\Horizon\Contracts\MetricsRepository
{
    /**
     * The Redis connection instance.
     *
     * @var \Illuminate\Contracts\Redis\Factory
     */
    public $redis;

    /**
     * Cache for all of the class names that have metrics measurements.
     *
     * @var array|null
     */
    protected $measuredJobs;

    /**
     * Cache for all of the queues that have metrics measurements.
     *
     * @var array|null
     */
    protected $measuredQueues;

    /**
     * Minutes to remember metrics.
     *
     * @var int
     */
    protected $rememberFor = 1440;

    /**
     * Create a new repository instance.
     *
     * @param  \Illuminate\Contracts\Redis\Factory  $redis
     * @return void
     */
    public function __construct(Factory $redis)
    {
        $this->redis = $redis;
    }

    /**
     * Get all of the class names that have metrics measurements.
     *
     * @return array
     */
    public function measuredJobs()
    {
        $classes = (array) $this->connection()->smembers('measured_jobs');

        $this->measuredJobs = collect($classes)->map(function ($class) {
            return preg_match('/job:(.*)$/', $class, $matches) ? $matches[1] : $class;
        })->all();

        return $this->measuredJobs;
    }

    /**
     * Get all of the queues that have metrics measurements.
     *
     * @return array
     */
    public function measuredQueues()
    {
        $queues = (array) $this->connection()->smembers('measured_queues');

        $this->measuredQueues = collect($queues)->map(function ($class) {
            return preg_match('/queue:(.*)$/', $class, $matches) ? $matches[1] : $class;
        })->all();

        return $this->measuredQueues;
    }

    public function metricsPerMinute($type = 'q', $for = null)
    {
        $now = Carbon::now();
        $minute = intdiv($now->timestamp, 60);
        $seconds = 60 + $now->timestamp % 60;

        $keys = [
            "metrics:$type:$minute",
            "metrics:$type:".($minute - 1),
        ];
        $metrics = collect($keys)->reduce(function ($key, $metrics) use ($for) {
            if (is_null($for)) {
                $items = $this->connection()->hgetall($key);
            } else {
                $items = $this->connection()->hmget($key, ["t:$for", "r:$for"]);
            }
            collect($items)->each(function ($v, $k) use (&$metrics) {
                switch ($k[0]) {
                    case 't':
                        $metrics[0] += (int) $v;
                        break;
                    case 'r':
                        $metrics[1] += (float) $v;
                }
            });

        }, [0, 0]);

        return [
            round($metrics[0] / $seconds / 60),
            $metrics[0] ? round($metrics[1] / $metrics[0] / 1000000) : 0
        ];
    }

    /**
     * Get the jobs processed per minute since the last snapshot.
     *
     * @return int
     */
    public function jobsProcessedPerMinute()
    {
        return $this->metricsPerMinute()[1];
    }

    /**
     * Get the application's total throughput since the last snapshot.
     *
     * @return int
     */
    public function throughput()
    {
        return $this->metricsPerMinute()[0];
    }

    /**
     * Get the throughput for a given job.
     *
     * @param  string $job
     * @return int
     */
    public function throughputForJob($job)
    {
        return $this->metricsPerMinute('j', $job)[0];
    }

    /**
     * Get the throughput for a given queue.
     *
     * @param  string $queue
     * @return int
     */
    public function throughputForQueue($queue)
    {
        return $this->metricsPerMinute('q', $queue)[0];
    }

    /**
     * Get the average runtime for a given job in milliseconds.
     *
     * @param  string $job
     * @return float
     */
    public function runtimeForJob($job)
    {
        return $this->metricsPerMinute('j', $job)[1];
    }

    /**
     * Get the average runtime for a given queue in milliseconds.
     *
     * @param  string $queue
     * @return float
     */
    public function runtimeForQueue($queue)
    {
        return $this->metricsPerMinute('q', $queue)[1];
    }

    protected function queueWithMaximux($metric)
    {
        $key = 'metrics:q:'.(intdiv(Carbon::now()->subMinute()->timestamp, 60));
        return collect($this->connection()->hgetall($key))->filter(function ($val, $key) use (&$metric) {
            return $key[0] === $metric;
        })->sort()->last();
    }

    /**
     * Get the queue that has the longest runtime.
     *
     * @return int
     */
    public function queueWithMaximumRuntime()
    {
        return $this->queueWithMaximux('r');
    }

    /**
     * Get the queue that has the most throughput.
     *
     * @return int
     */
    public function queueWithMaximumThroughput()
    {
        return $this->queueWithMaximux('t');
    }

    /**
     * Increment the metrics information for a job.
     *
     * @param  string $job
     * @param  float $runtime
     * @return void
     */
    public function incrementJob($job, $runtime)
    {
        if (! isset($this->measuredJobs[$job])) {
            $this->connection()->sadd('measured_jobs', [$job]);
        }
        $this->connection()->eval(LuaScripts::incrementMetrics(), 1,
            'measures:j:'.intdiv(Carbon::now()->timestamp, 60),
            't:'.$job, 'r:'.$job, $runtime * 1000000, $this->rememberFor * 60);
    }

    /**
     * Increment the metrics information for a queue.
     *
     * @param  string $queue
     * @param  float $runtime
     * @return void
     */
    public function incrementQueue($queue, $runtime)
    {
        if (! isset($this->measuredQueues[$queue])) {
            $this->connection()->sadd('measured_queues', [$queue]);
        }
        $this->connection()->eval(LuaScripts::incrementMetrics(), 1,
            'measures:q:'.intdiv(Carbon::now()->timestamp, 60),
            't:'.$queue, 'r:'.$queue, $runtime * 1000000, 3600 * 24);
    }

    /**
     * Get all of the snapshots for the given key.
     *
     * @param  string  $key
     * @return array
     */
    protected function snapshotsFor($key)
    {
        $this->snapshot();

        return collect($this->connection()->zrange('snapshot:'.$key, 0, -1, ['WITHSCORES']))
            ->map(function ($snapshot) {
                return (object) json_decode($snapshot, true);
            })->values()->all();
    }

    /**
     * Get all of the snapshots for the given job.
     *
     * @param  string $job
     * @return array
     */
    public function snapshotsForJob($job)
    {
        return $this->snapshotsFor('job:'.$job);
    }

    /**
     * Get all of the snapshots for the given queue.
     *
     * @param  string $queue
     * @return array
     */
    public function snapshotsForQueue($queue)
    {
        return $this->snapshotsFor('queue:'.$queue);
    }

    /**
     * Store a snapshot of the metrics information.
     *
     * @return void
     */
    public function snapshot()
    {
        $lastSnapshot = $this->connection()->get('last_snapshot') ?? intdiv(Carbon::now()->subMinute($this->rememberFor)->timestamp, 60);
        $now = intdiv(Carbon::now()->timestamp, 60);
        for ($time = $lastSnapshot + 1; $time < $now; $time++) {
            $this->storeSnapshot($time, 'metrics:q:'.$time, 'queue:');
            $this->storeSnapshot($time, 'metrics:j:'.$time, 'job:');
        }
        $this->connection()->set('last_snapshot', $now - 1);
    }

    protected function storeSnapshot($time, $metrics, $prefix)
    {
        $items = $this->connection()->hgetall($metrics);
        $snapshots = [];
        foreach ($items as $key => $value) {
            preg_match('/^(.):(.*)$/', $key, $parts);
            $snapshots[$parts[2]][$parts[1] === 'r' ? 'runtime' : 'throughput'] = $value;
        }
        $this->connection()->pipeline(function ($pipe) use ($snapshots, $time, $prefix) {
            foreach ($snapshots as $key => $value) {
                $pipe->zadd(
                    'snapshot:'.$prefix.$key, ($time + 1) * 60, json_encode([
                        'throughput' => $value['throughput'],
                        'runtime' => $value['runtime'] ? $value['runtime'] / $value['throughput'] / 1000000 : 0,
                        'time' => ($time + 1) * 60,
                    ])
                );
            }
        });
    }

    /**
     * Attempt to acquire a lock to monitor the queue wait times.
     *
     * @return bool
     */
    public function acquireWaitTimeMonitorLock()
    {
        return app(Lock::class)->get('monitor:time-to-clear');
    }

    /**
     * Clear the metrics for a key.
     *
     * @param  string $key
     * @return void
     */
    public function forget($key)
    {
        $this->connection()->del($key);
    }

    /**
     * Get the Redis connection instance.
     *
     * @return \Illuminate\Redis\Connections\Connection
     */
    public function connection()
    {
        return $this->redis->connection('horizon');
    }
}