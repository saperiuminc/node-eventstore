import { Redis, RedisOptions } from "ioredis";

 // TODO: Add more types of ConnectionOptions
 export interface EventstoreOptions {
    type: 'inmemory' | 'mysql' | 'redis' | 'dynamodb' | 'azuretable' | 'elasticsearch' | 'tingodb' | 'dynamodb',
    host: string;
    port: number;
    user: string;
    password: string;
    database: string;
    connectionPoolLimit: number;
    redisCreateClient?(type: 'client' | 'subscriber' | 'bclient', redisOpts?: RedisOptions): Redis;
    listStore: MySqlConnectionOptions;
    projectionStore: MySqlConnectionOptions;
    enableProjection: boolean;
    enableProjectionEventStreamBuffer: boolean;
    eventCallbackTimeout: number;
    lockTimeToLive: number;
    pollingTimeout: number; // optional,
    pollingMaxRevisions: number;
    errorMaxRetryCount: number;
    errorRetryExponent: number;
    playbackEventJobCount: number;
    context: string;
}

export class Eventstore {
    init(callback: OnCallback<void>);
}

export class EventstoreWithProjections extends Eventstore {
    project(projectionConfiguration: ProjectionConfiguration, callback: Function);
    subscribe(query: EventstoreQuery, revision: number, onEventCallback: OnCallback<EventstoreEvent>, onErrorCallback)
}

export interface MySqlConnectionOptions {
    type: 'mysql',
    connection: {
        host: string;
        port: number;
        user: string;
        password: string;
        database: string;   
    }
    pool: {
        min: number;
        max: number;
    }
}

export interface EventstoreQuery {
    context: string;
    aggregate: string;
    aggregateId: string;
}

export interface PlaybackListConfiguration {
    name: string;
    fields: Array<EventstorePlaybackListField>
}

export interface EventstorePlaybackListFilter {
    field: string;
    group?: string;
    groupBooleanOperator?: "or"|"and"|null;
    operator: "is"|"any"|"range"|"dateRange"|"contains"|"arrayContains"|"startsWith"|"endsWith"|"exists"|"notExists";
    value: string;
    from?: string;
    to?: string;
}

export interface EventstorePlaybackListSort {
    field: string;
    sort: "ASC"|"DESC";
}

export interface EventstorePlaybackListSecondaryKey {
    name: string;
    sort: "ASC"|"DESC";
}

export interface EventstorePlaybackListField {
    type: string;
    name: string;
}

export interface ProjectionConfiguration {
    projectionId: string;
    projectionName: string;
    playbackInterface: Object;
    query: EventstoreQuery;
    partitionBy: '' | 'stream' | 'function';
    outputState: boolean;
    playbackList: PlaybackListConfiguration;
    eventCallbackTimeout: number;
}

// TODO: add EventstoreEvent
export interface EventstoreEvent {

}

// TODO: add state list

// TODO: add projection store

export interface OnCallback<T> { (error: Error, data: T): void }