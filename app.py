from decimal import Decimal
from random import randint
from uuid import uuid4

from flask import Flask, request, jsonify, render_template, url_for
from flask_cors import CORS
from faker import Faker
from money.money import Money
from money.currency import Currency

from miniq import Miniq


def round_up(x, n):
    """round_up(x: int, n: int) -> int
    
    Rounds x _up_ to the nearest multiple of n.
    """
    return (x // n + int(x % n != 0)) * n


app = Flask(__name__)
cors = CORS(app)
faker = Faker()
miniq = Miniq('localhost:3000')

inventory = {
    str(uuid4()): randint(0, 10)
    for _ in range(100)
}
products = {
    key: {
        'sku': key,
        'title': faker.name(),
        'description': faker.sentence(),
        'price': Money(round_up(Decimal(randint(5000, 250000)) / 100, 5), Currency.DKK),
    }
    for key in inventory.keys()
}
viewings = {}  # {str(UUID): int}
reservations = {}


def get_popular_products(limit):
    return list(sorted(
        [
            get_product_details(sku)
            for sku, product
            in products.items()
            if inventory[sku] > 0
        ],
        key=lambda product_details: product_details['views'],
        reverse=True,
    ))[0:limit]


def get_product_details(sku):
    return {
        'product': products[sku],
        'inventory': inventory[sku],
        'url': url_for('view_product_details', sku=sku),
        'views': viewings.get(sku, 0)
    }


@app.route('/')
def view_home():
    popular_products = get_popular_products(10)
    return render_template('home.html', popular_products=popular_products)


@app.route('/api/popular-products')
def api_popular_products():
    limit = int(request.args.get('limit', 10))
    return jsonify({'data': get_popular_products(limit)})


@app.route('/products/<string:sku>')
def view_product_details(sku):
    viewings[sku] = viewings.get(sku, 0) + 1
    return render_template('product_details.html', product_details=get_product_details(sku))


@app.route('/api/products/<string:sku>')
def api_product_details(sku):
    return jsonify({'data': get_product_details(sku)})


@app.route('/api/inventory')
def api_inventory():
    return jsonify({'data': inventory})


@app.route('/api/inventory/<string:sku>')
def api_inventory_sku(sku):
    return jsonify({'data': inventory[sku]})
