import {
  type JobArgs,
  type JobId,
  type JobRecord,
  type JobResult,
  JobState,
  type WorkerCommandDescriptor,
  type WorkerId,
  type WorkerInfo,
  WorkerState,
} from '@queuebone/core';
import { type ClientWorkerUpdateOptions } from '../backend.js';

export type UpdateJobAPI = {
  Params: { id: JobId; attempt: number };
  Body: {
    result?: JobResult;
    state?: JobState.Succeeded | JobState.Failed;
    progress?: number;
    metadata?: Record<string, any>;
  };
  Reply: {
    metadata?: Record<string, any>;
  };
};

export const updateJobSchema = {
  params: {
    type: 'object',
    required: ['id', 'attempt'],
    properties: {
      id: { type: 'number' },
      attempt: { type: 'number' },
    },
  },
  body: {
    anyOf: [
      {
        type: 'object',
        properties: {
          metadata: {
            type: 'object',
            properties: {},
          },
        },
      },
      {
        type: 'object',
        properties: {
          progress: { type: 'number' },
        },
      },
      {
        type: 'object',
        properties: {
          state: {
            type: 'string',
            enum: [JobState.Succeeded, JobState.Failed],
          },
          result: {
            type: 'object',
            // properties: {},
          },
        },
      },
    ],
  },
};

export type RegisterWorkerAPI = {
  Reply: WorkerInfo;
};

export const registerWorkerSchema = {};

export type WorkerAPI = {
  Params: { id: WorkerId };
};

export const workerSchema = {
  params: {
    type: 'object',
    required: ['id'],
    properties: {
      id: { type: 'number' },
    },
  },
};

export type UpdateWorkerAPI = {
  Params: { id: WorkerId };
  Body: ClientWorkerUpdateOptions;
  Reply: WorkerInfo;
};

export const updateWorkerSchema = {
  params: {
    type: 'object',
    required: ['id'],
    properties: {
      id: { type: 'number' },
    },
  },
  body: {
    type: 'object',
    required: ['state'],
    properties: {
      state: {
        type: 'string',
        enum: [WorkerState.Online, WorkerState.Idle, WorkerState.Busy],
      },
    },
  },
};

export type UnregisterWorkerAPI = {
  Params: { id: WorkerId };
};

export const unregisterWorkerSchema = {
  params: {
    type: 'object',
    required: ['id'],
    properties: {
      id: { type: 'number' },
    },
  },
};

export type AssignNextJobAPI = {
  Params: { id: WorkerId };
  Body: {
    taskNames: string[];
    timeout: number;
    options: {
      minPriority?: number;
    };
  };
  Reply: JobRecord<JobArgs> | void;
};

export const assignNextJobSchema = {
  params: {
    type: 'object',
    required: ['id'],
    properties: {
      id: { type: 'number' },
    },
  },
  body: {
    type: 'object',
    properties: {
      taskNames: { type: 'array', items: { type: 'string' } },
      timeout: { type: 'number' },
      options: {
        type: 'object',
        properties: {
          minPriority: { type: 'number' },
        },
      },
    },
  },
};

export type CheckWorkerInboxAPI = {
  Params: { id: WorkerId };
  Body: ClientWorkerUpdateOptions;
  Reply: WorkerCommandDescriptor[];
};

export const checkWorkerInboxSchema = {
  params: {
    type: 'object',
    required: ['id'],
    properties: {
      id: { type: 'number' },
    },
  },
  body: {
    type: 'object',
    required: ['state'],
    properties: {
      state: {
        type: 'string',
        enum: [WorkerState.Online, WorkerState.Idle, WorkerState.Busy],
      },
    },
  },
};
