import express from 'express'
import { json } from 'body-parser'
import { KafkaClient, Producer } from 'kafka-node'
import { Cart } from './Cart';

const app = express()

const kafkaClient = new KafkaClient({ kafkaHost: 'localhost:9092' })
const kakfaProducer = new Producer(kafkaClient)

const carts: {[id: number]: Cart} = {}

interface Command {
    type: string
}

interface CommandHandler<T extends Command> {
    supports(c: Command): c is T;
    handle(c: T): void;
}

class Product {
    constructor(public name: string) {}

    toJson() {
        return JSON.stringify(this)
    }
}

class ProductAddCommand implements Command {
    public type = 'PRODUCT_ADD'

    constructor(public cartId: number, public product: Product) {
    }
}

class ProductRemoveCommand implements Command {
    public type = 'PRODUCT_REMOVE'

    constructor(public cartId: number, public product: Product) {
    }
}

class ProductAddCommandHandler implements CommandHandler<ProductAddCommand> {

    constructor(private kafkaProducer: Producer) {
    }

    supports(c: Command): c is ProductAddCommand {
        return c.type === 'PRODUCT_ADD'
    }

    handle(c: ProductAddCommand): void {
        this.kafkaProducer.send([{
            topic: `PRODUCT_ADD`,
            messages: [JSON.stringify({ cartId: c.cartId, product: c.product.toJson() })]
        }], () => {
            console.log('Sent')
        })
    }
}

class ProductRemoveCommandHandler implements CommandHandler<ProductRemoveCommand> {

    constructor(private kafkaProducer: Producer) {
    }

    supports(c: Command): c is ProductAddCommand {
        return c.type === 'PRODUCT_REMOVE'
    }

    handle(c: ProductAddCommand): void {
        this.kafkaProducer.send([{
            topic: `PRODUCT_REMOVE`,
            messages: [JSON.stringify({ cartId: c.cartId, product: c.product.toJson() })]
        }], () => {
            console.log('Sent')
        })
    }
}

class CommandBus {

    private handlers = [
        new ProductAddCommandHandler(kakfaProducer),
        new ProductRemoveCommandHandler(kakfaProducer)
    ]

    handle(cmd: Command) {
        for (let h of this.handlers) {
            if (h.supports(cmd)) {
                h.handle(cmd);
                break;
            }
        }
    }
}

const bus = new CommandBus()

app.post('/cart/:cartId/product/:productId', json(), (req, res) => {
    const { cartId, productId } = req.params;

    let cart: Cart
    if (carts[cartId] == null) {
        cart = new Cart(cartId, kafkaClient)
        carts[cartId] = cart
    }

    bus.handle(new ProductAddCommand(cartId, new Product('testing')))
    res.json({ message: 'done', cartId: cartId })
})

app.get('/cart/:cartId', (req, res) => {
    const { cartId } = req.params;

    if (carts[cartId]) {
        return res.json({
            items: carts[cartId].items
        })
    }

    res.json({ message: 'done' })
})

app.listen(3000, () => console.log('Ready on 3000'))