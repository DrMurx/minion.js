import {
  type JobArgs,
  type JobId,
  type JobRecord,
  type JobResult,
  JobState,
  type WorkerCommandDescriptor,
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
  Body: {
    name: string;
    passphrase: string;
  };
  Reply: {
    token: string;
    info: WorkerInfo;
  };
};

export const registerWorkerSchema = {
  body: {
    type: 'object',
    required: ['name', 'passphrase'],
    properties: {
      name: { type: 'string' },
      passphrase: { type: 'string' },
    },
  },
};

export type UpdateWorkerAPI = {
  Body: ClientWorkerUpdateOptions;
  Reply: WorkerInfo;
};

export const updateWorkerSchema = {
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

export type AssignNextJobAPI = {
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
  Body: ClientWorkerUpdateOptions;
  Reply: WorkerCommandDescriptor[];
};

export const checkWorkerInboxSchema = {
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

export type PingAPI = {
  Body: {
    name?: string;
    passphrase?: string;
  };
  Reply: {
    status: string;
  };
};

export const pingSchema = {
  body: {
    type: 'object',
    properties: {
      name: { type: 'string' },
      passphrase: { type: 'string' },
    },
  },
};
