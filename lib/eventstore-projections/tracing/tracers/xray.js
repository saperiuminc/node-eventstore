/* eslint-disable require-jsdoc */
const util = require('util');
const Tracer = require('opentracing').Tracer;
const Span = require('opentracing').Span;
const _ = require('lodash');
const AWSXRay = require('aws-xray-sdk');


/**
 * JaegerTracer constructor
 * @constructor
 */
function JaegerTracer() {
    // init here
}


util.inherits(JaegerTracer, Tracer);

JaegerTracer.prototype = Tracer.prototype;

JaegerTracer.prototype.startSpan = function() {
    throw new Error('')
}


/**
 * JaegerSpan constructor
 * @param {JaegerSpan} tracer the jaeger tracer
 * @constructor
 */
function JaegerSpan(tracer) {
    this._tracer = tracer;
}


util.inherits(JaegerSpan, Span);

JaegerSpan.prototype = Span.prototype;

JaegerSpan.prototype.finish = function() {
    // use this._tracer to get the parent tracer
    throw new Error('not implemented')
}


module.exports = {
    Tracer: JaegerTracer,
    Span: JaegerSpan
};