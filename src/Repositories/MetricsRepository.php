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
                $items = $this->connection()->hmget($key, ["$for:t", "$for:r"]);
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
            $metrics[0] ? round($metrics[1] / $metrics[0]) : 0
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

    /**
     * Get the queue that has the longest runtime.
     *
     * @return int
     */
    public function queueWithMaximumRuntime()
    {
        // TODO: Implement queueWithMaximumRuntime() method.
    }

    /**
     * Get the queue that has the most throughput.
     *
     * @return int
     */
    public function queueWithMaximumThroughput()
    {
        // TODO: Implement queueWithMaximumThroughput() method.
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
            $job.':t', $job.':r', str_replace(',', '.', $runtime), 3600 * 24);
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
            $queue.':t', $queue.':r', str_replace(',', '.', $runtime), 3600 * 24);
    }

    /**
     * Get all of the snapshots for the given job.
     *
     * @param  string $job
     * @return array
     */
    public function snapshotsForJob($job)
    {
        // TODO: Implement snapshotsForJob() method.
    }

    /**
     * Get all of the snapshots for the given queue.
     *
     * @param  string $queue
     * @return array
     */
    public function snapshotsForQueue($queue)
    {
        // TODO: Implement snapshotsForQueue() method.
    }

    /**
     * Store a snapshot of the metrics information.
     *
     * @return void
     */
    public function snapshot()
    {
        // TODO: Implement snapshot() method.
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
        // TODO: Implement forget() method.
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