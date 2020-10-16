/* eslint-disable require-jsdoc */
const util = require('util');
const Tracer = require('opentracing').Tracer;
const Span = require('opentracing').Span;
const AWSXRay = require('aws-xray-sdk');
var ns = AWSXRay.getNamespace();

const rootSegmentName = process.env.AWS_XRAY_TRACING_NAME || 'default';

class XraySpan extends Span {
    constructor(tracer, segment, parentSegment) {
        super();
        this._tracer = tracer;
        this._segment = segment;
        this._parentSegment = parentSegment;
    }

    context() {
        const context = super.context();
        context.segment = this._segment;
        context.parentSegment = this._parentSegment;
        return context;
    }

    getSegment() {
        return this._segment;
    }

    getTracer() {
        return this._tracer;
    }

    finish() {
        this._segment.close();
        if (this._parentSegment.name == rootSegmentName) {
            this._parentSegment.close();
        }
    }
}

class XrayTracer extends Tracer {
    constructor() {
        super();
    }

    startSpan(spanName, spanOptions) {
        try {
            const doStartSpan = function() {
                if (spanOptions && spanOptions.childOf) {
                    const parentSpanContext = spanOptions.childOf;
                    const parentSegment = parentSpanContext.segment;

                    const subSegment = parentSegment.addNewSubsegment(spanName);
                    const span = new XraySpan(this, subSegment, parentSegment);

                    return span;
                } else {
                    // no parent. so create a parent segment
                    const parentSegment = new AWSXRay.Segment(rootSegmentName);
                    AWSXRay.setSegment(parentSegment);

                    const subSegment = parentSegment.addNewSubsegment(spanName);
                    const span = new XraySpan(this, subSegment, parentSegment);

                    return span;
                }
            }
            
            if (!ns.active) {
                return ns.runAndReturn(() => {
                    return doStartSpan();
                });
            } else {
                return doStartSpan();
            }
        } catch (error) {
            console.error('error in startSpan with error:', error);
        }
    }
}


module.exports = XrayTracer;