const TracerFactory = {
    /**
     * @param {String} tracerName the unique name of the tracer
     * @returns {import('opentracing').Tracer} - returns a Tracer object
     */
    getTracer: function(tracerName) {
        switch (tracerName) {
            case 'jaeger': {
                const jaeger = require('jaeger-client');
                const tracer = jaeger.initTracerFromEnv();
                return tracer;
            }

            case 'xray': {
                const XrayTracer = require('./tracers/xray');
                const tracer = new XrayTracer();
                return tracer;
            }

            default:
                throw new Error('invalid tracerName');
        }
    }
}

module.exports = TracerFactory;