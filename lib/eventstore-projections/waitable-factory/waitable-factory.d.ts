import { WaitableConsumer } from "./waitable-consumer";
import { WaitableFactoryOptions } from "./typings";
import { WaitableProducer } from "./waitable-producer";

export class WaitableFactory {
    constructor(options: WaitableFactoryOptions);
    createWaitableConsumer(topics: string[]): Promise<WaitableConsumer>;
    createWaitableProducer(): WaitableProducer;
}