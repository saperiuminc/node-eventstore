/* eslint-disable require-jsdoc */

class Event {
    constructor(eventstream, event) {
        this.aggregateId = eventstream.aggregateId;
        this.aggregate = eventstream.aggregate;
        this.context = eventstream.context;
        this.streamRevision = event.streamRevision;
        this.eventId = event.eventId;
        this.eventAt = event.eventAt;
        this.payload = event;
        this.payload.aggregate = eventstream.aggregate;

        // Add Info that's needed by event stream listeners (i.e. Bounded context, Kibana, etc)
        this.payload.eventId = event.eventId;
        this.payload.eventAt = event.eventAt;
        this.payload.streamRevision = event.streamRevision;
    }
}

module.exports = Event;