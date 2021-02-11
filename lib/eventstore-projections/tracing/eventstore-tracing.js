if (process.env.ENABLE_TRACING == 'true') {
    const cls = require('cls-hooked');
    const tracer = require('./eventstore-tracer-factory').getTracer(process.env.TRACER_NAME || 'jaeger');
    const nsTracer = (cls.getNamespace('__NS_TRACER__') || cls.createNamespace('__NS_TRACER__'));
    
    require('bluebird');
    require('cls-bluebird')(nsTracer);
    require('mysql');
    require('cls-mysql')(nsTracer);

    const shimmer = require('shimmer');
    (function patchEventstoreProjection(proto) {
        // _processProjection is async
        shimmer.wrap(proto, '_processProjection', function(original) {
            return function() {
                // if no active namespace context yet (run or runAndRetrun has not yet ran) 
                // then run a context
                const spanName = `eventstore-extensions._processProjection`;
                if (!nsTracer.active) {
                    return nsTracer.runAndReturn(async (context) => {
                        // _processProjection is async
                        const span = tracer.startSpan(spanName);
                        nsTracer.set('parentSpan', span);
                        const result = await original.apply(this, arguments);
                        span.finish();

                        return result;
                    });
                } else {
                    return original.apply(this, arguments);
                }
            };
        });

        // _playbackEvent is async
        shimmer.wrap(proto, '_playbackEvent', function(original) {
            return function() {
                if (nsTracer.active) {
                    return nsTracer.runAndReturn(async (context) => {
                        const spanName = `eventstore._playbackEvent`;
                        
                        const parentSpan = nsTracer.get('parentSpan');

                        let childOf;
                        if (parentSpan) {
                            childOf = parentSpan.context();
                        }
                        
                        const span = tracer.startSpan(spanName, {
                            childOf: childOf
                        });

                        nsTracer.set('parentSpan', span);

                        const result = await original.apply(this, arguments);
                        span.finish();

                        return result;
                    });

                } else {
                    return original.apply(this, arguments);
                }
            };
        });
    })(require('../eventstore-projection').prototype);

    (function patchEventstore(proto, whitelist) {
        var raFuncNames = Object.keys(proto).filter((x) => !whitelist || whitelist.includes(x));
        raFuncNames.forEach((funcName) => {
            shimmer.wrap(proto, funcName, function(original) {
                return function() {
                    if (nsTracer.active) {
                        return nsTracer.runAndReturn((context) => {
                            const spanName = `eventstore.${funcName}`;
                            var originalCallback = arguments[arguments.length - 1];
                            const childOf = nsTracer.get('parentSpan') ? nsTracer.get('parentSpan').context() : undefined;
                            const span = tracer.startSpan(spanName, {
                                childOf: childOf
                            });

                            nsTracer.set('parentSpan', span);

                            arguments[arguments.length - 1] = nsTracer.bind(function() {
                                // processJobCallback is async
                                span.finish();
                                return originalCallback.apply(this, arguments);
                            });

                            // run original function w/patched callback
                            return original.apply(this, arguments);
                        });

                    } else {
                        return original.apply(this, arguments);
                    }
                };
            });
        });
    })(require('../eventstore-projection').prototype, ['getEventStream', 'getLastEventAsStream', 'getFromSnapshot', 'createSnapshot', 'getEvents', 'getEventsByRevision', 'getEventsSince', 'getLastEvent']);

    (function patchEventStream(proto, whitelist) {
        var raFuncNames = Object.keys(proto).filter((x) => !whitelist || whitelist.includes(x));
        raFuncNames.forEach((funcName) => {
            shimmer.wrap(proto, funcName, function(original) {
                return function() {
                    if (nsTracer.active) {
                        return nsTracer.runAndReturn((context) => {
                            const spanName = `eventStream.${funcName}`;
                            const span = tracer.startSpan(spanName, {
                                childOf: nsTracer.get('parentSpan') ? nsTracer.get('parentSpan').context() : undefined
                            });

                            // nsTracer.set('parentSpan', span);

                            var originalCallback = arguments[arguments.length - 1];

                            // check if last argument is a function
                            if (typeof originalCallback === "function") {
                                arguments[arguments.length - 1] = nsTracer.bind(function() {
                                    // processJobCallback is async
                                    span.finish();
                                    return originalCallback.apply(this, arguments);
                                });
                            } else {
                                span.finish();
                            }

                            // run original function w/patched callback
                            return original.apply(this, arguments);
                        });

                    } else {
                        return original.apply(this, arguments);
                    }
                };
            });
        });
    })(require('../../eventStream').prototype, ['addEvent', 'addEvents', 'commit']);

    (function patchPlaybackList(proto, whitelist) {
        var raFuncNames = Object.keys(proto).filter((x) => !whitelist || whitelist.includes(x));
        raFuncNames.forEach((funcName) => {
            shimmer.wrap(proto, funcName, function(original) {
                return function() {
                    if (nsTracer.active) {
                        return nsTracer.runAndReturn((context) => {
                            const spanName = `evenstore-playbacklist.${funcName}`;
                            const span = tracer.startSpan(spanName, {
                                childOf: nsTracer.get('parentSpan') ? nsTracer.get('parentSpan').context() : undefined
                            });

                            nsTracer.set('parentSpan', span);

                            var originalCallback = arguments[arguments.length - 1];

                            // check if last argument is a function
                            if (typeof originalCallback === "function") {
                                arguments[arguments.length - 1] = nsTracer.bind(function() {
                                    span.finish();
                                    return originalCallback.apply(this, arguments);
                                });
                            } else {
                                span.finish();
                            }

                            // run original function w/patched callback
                            return original.apply(this, arguments);
                        });

                    } else {
                        return original.apply(this, arguments);
                    }
                };
            });
        });
    })(require('../eventstore-playback-list').prototype, ['createList', 'query', 'get', 'add', 'update', 'delete']);

    (function patchMysqlPool(proto, whitelist) {
        var raFuncNames = Object.keys(proto).filter((x) => !whitelist || whitelist.includes(x));
        raFuncNames.forEach((funcName) => {
            shimmer.wrap(proto, funcName, function(original) {
                return function() {
                    if (nsTracer.active) {
                        return nsTracer.runAndReturn((context) => {
                            const spanName = `mysql-pool.${funcName}`;
                            const span = tracer.startSpan(spanName, {
                                childOf: nsTracer.get('parentSpan') ? nsTracer.get('parentSpan').context() : undefined
                            });

                            // nsTracer.set('parentSpan', span);

                            var originalCallback = arguments[arguments.length - 1];

                            // check if last argument is a function
                            if (typeof originalCallback === "function") {
                                arguments[arguments.length - 1] = nsTracer.bind(function() {
                                    span.finish();
                                    return originalCallback.apply(this, arguments);
                                });
                            } else {
                                span.finish();
                            }

                            // run original function w/patched callback
                            return original.apply(this, arguments);
                        });

                    } else {
                        return original.apply(this, arguments);
                    }
                };
            });
        });
    })(require('mysql/lib/Pool').prototype, ['getConnection']);

    (function patchMysqlConnection(proto, whitelist) {
        var raFuncNames = Object.keys(proto).filter((x) => !whitelist || whitelist.includes(x));
        raFuncNames.forEach((funcName) => {
            shimmer.wrap(proto, funcName, function(original) {
                return function() {
                    if (nsTracer.active) {
                        return nsTracer.runAndReturn((context) => {
                            const spanName = `mysql-connection.${funcName}`;
                            const span = tracer.startSpan(spanName, {
                                childOf: nsTracer.get('parentSpan') ? nsTracer.get('parentSpan').context() : undefined
                            });

                            span.log({
                                arguments: arguments
                            }, Date.now());
                            
                            // nsTracer.set('parentSpan', span);

                            var originalCallback = arguments[arguments.length - 1];

                            // check if last argument is a function
                            if (typeof originalCallback === "function") {
                                arguments[arguments.length - 1] = nsTracer.bind(function() {
                                    span.finish();
                                    return originalCallback.apply(this, arguments);
                                });
                            } else {
                                span.finish();
                            }

                            // run original function w/patched callback
                            return original.apply(this, arguments);
                        });

                    } else {
                        return original.apply(this, arguments);
                    }
                };
            });
        });
    })(require('mysql/lib/Connection').prototype, ['query', 'beginTransaction', 'commit', 'rollback', 'release']);
}

module.exports = {};