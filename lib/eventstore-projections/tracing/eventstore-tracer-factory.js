const tracerFactory = {
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
                const a = new XrayTracer();
                console.log(a);
                return a;
            }

            default:
                throw new Error('invalid tracerName');
        }
    }
}

module.exports = tracerFactory;