import { KafkaClient, Consumer } from "kafka-node";

export class Cart {

    private _items: any[] = []

    private readonly consumer: Consumer;

    public get items() {
        return this._items
    }

    constructor(private id: string, kafkaClient: KafkaClient) {
        const _this = this
        this.consumer = new Consumer(kafkaClient, [{
            topic: `PRODUCT_ADD`,
            offset: 0,
            partition: 0
        }], {
            fromOffset: true
        });

        this.consumer.on('message', (message) => {
            console.log(`[Cart:${this.id}] on receive message`, message.value)
            const { value } = message
            const { cartId, product } = JSON.parse(value.toString())

            if (cartId === _this.id) {
                console.log(`[Cart:${this.id}] Add to cart`)
                _this._items.push(product)
            }
        })
    }
}