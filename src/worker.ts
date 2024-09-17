import { Job } from './job.js';
import type { Minion } from './minion.js';
import type { DequeueOptions, MinionCommand, MinionStatus, WorkerInfo, WorkerOptions } from './types.js';

interface JobStatus {
  job: Job;
  promise: Promise<void>;
}

/**
 * Minion worker class.
 */
export class Worker {
  /**
   * Worker status.
   */
  public status: MinionStatus;

  /**
   * Registered commands.
   */
  private commands: Record<string, MinionCommand>;

  private runningJobs: JobStatus[] = [];
  private lastCommandAt = 0;
  private lastHeartbeatAt = 0;
  private lastRepairAt = 0;
  private _isRunning = false;
  private stopPromises: Array<() => void> = [];

  private _id: number | undefined = undefined;
  private minion: Minion;

  constructor(minion: Minion, options: WorkerOptions = {}) {
    this.commands = {jobs: jobsCommand, ...options.commands};
    const status = (this.status = {
      commandInterval: 10000,
      dequeueTimeout: 5000,
      heartbeatInterval: 300000,
      jobs: 4,
      performed: 0,
      queues: ['default'],
      repairInterval: 21600000,
      spare: 1,
      spareMinPriority: 1,
      type: 'Node.js',
      ...options.status
    });
    status.repairInterval -= Math.ceil(Math.random() * (status.repairInterval / 2));

    this.minion = minion;
  }

  /**
   * Register a worker remote control command.
   */
  addCommand(name: string, fn: MinionCommand): void {
    this.commands[name] = fn;
  }

  /**
   * Wait a given amount of time in milliseconds for a job, dequeue job object and transition from `pending` to
   * `running` state, or return `null` if queues were empty.
   */
  async dequeue(wait = 0, options: DequeueOptions = {}): Promise<Job | null> {
    const id = this._id;
    if (id === undefined) return null;

    const job = await this.minion.backend.getNextJob(id, Object.keys(this.minion.tasks), wait, options);
    return job === null ? null : new Job(this.minion, job.id, job.args, job.retries, job.taskName);
  }

  /**
   * Worker id.
   */
  get id(): number | null {
    return this._id ?? null;
  }

  /**
   * Get worker information.
   */
  async getInfo(): Promise<WorkerInfo | null> {
    const id = this._id;
    if (id === undefined) return null;
    const list = await this.minion.backend.getWorkers(0, 1, {ids: [id]});
    return list.workers[0];
  }

  /**
   * Register worker or send heartbeat to show that this worker is still alive.
   */
  async register(): Promise<this> {
    const id = await this.minion.backend.registerWorker(this._id, {status: this.status});
    if (this._id === undefined) this._id = id;
    return this;
  }

  /**
   * Unregister worker.
   */
  async unregister(): Promise<this> {
    if (this._id === undefined) return this;
    await this.minion.backend.unregisterWorker(this._id);
    this._id = undefined;
    return this;
  }

  /**
   * Start worker.
   */
  async start(): Promise<this> {
    if (this.isRunning === true) return this;

    this.mainLoop().catch(error => console.error(error));
    this._isRunning = true;

    return this;
  }

  /**
   * Stop worker.
   */
  async stop(): Promise<void> {
    if (this.isRunning === false) return;
    return new Promise(resolve => this.stopPromises.push(resolve));
  }

  /**
   * Check if worker is currently running.
   */
  get isRunning(): boolean {
    return this._isRunning;
  }

  /**
   * Process worker remote control commands.
   */
  async processCommands(): Promise<void> {
    const id = this._id;
    if (id === undefined) return;

    const commands = await this.minion.backend.getWorkerNotifications(id);
    for (const [command, ...args] of commands) {
      const fn = this.commands[command];
      if (fn !== undefined) await fn(this, ...args);
    }
  }


  protected async mainLoop(): Promise<void> {
    const status = this.status;
    const stop = this.stopPromises;

    while (stop.length === 0 || this.runningJobs.length > 0) {
      const options: DequeueOptions = {queueNames: status.queues};

      await this.maintenance(status);

      // Check if jobs are finished
      const before = this.runningJobs.length;
      const runningJobs = (this.runningJobs = this.runningJobs.filter(jobStatus => jobStatus.job.isFinished === false));
      status.performed += before - runningJobs.length;

      // Job limit has been reached
      const {jobs, spare} = status;
      if (runningJobs.length >= jobs + spare) await Promise.race(runningJobs.map(jobStatus => jobStatus.promise));
      if (runningJobs.length >= jobs) options.minPriority = status.spareMinPriority;

      // Worker is stopped
      if (stop.length > 0) continue;

      // Try to get more jobs
      const job = await this.dequeue(status.dequeueTimeout, options);
      if (job === null) continue;
      runningJobs.push({job, promise: job.perform()});
    }

    await this.unregister();
    this.stopPromises = [];
    stop.forEach(resolve => resolve());
    this._isRunning = false;
  }

  protected async maintenance(status: MinionStatus): Promise<void> {
    // Send heartbeats in regular intervals
    if (this.lastHeartbeatAt + status.heartbeatInterval < Date.now()) {
      await this.register();
      this.lastHeartbeatAt = Date.now();
    }

    // Process worker remote control commands in regular intervals
    if (this.lastCommandAt + status.commandInterval < Date.now()) {
      await this.processCommands();
      this.lastCommandAt = Date.now();
    }

    // Repair in regular intervals (randomize to avoid congestion)
    if (this.lastRepairAt + status.repairInterval < Date.now()) {
      await this.minion.repair();
      this.lastRepairAt = Date.now();
    }
  }
}

// Remote control commands need to validate arguments carefully
async function jobsCommand(worker: Worker, num: any) {
  const jobs = parseInt(num);
  if (isNaN(jobs) === false) worker.status.jobs = jobs;
}
