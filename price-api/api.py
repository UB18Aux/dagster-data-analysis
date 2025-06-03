from fastapi import FastAPI
from datetime import datetime, time
import random
import uvicorn
import numpy as np

app = FastAPI(title="Market Item Price API", version="1.0.0")

TIME_CONFIGS = {
    "lunch": {
        "start": time(11, 0),
        "end": time(13, 0),
    },
    "afternoon": {
        "start": time(13, 0),
        "end": time(18, 0),
    },
    "dinner": {
        "start": time(18, 0),
        "end": time(22, 0),
    },
    "night": {
        "start": time(22, 0),
        "end": time(11, 0),
    },
}

ITEM_CONFIG = {
    "1": ("Regular Pizza", "Meal"),
    "2": ("Large Pizza", "Meal"),
    "3": ("Tiny Pizza", "Meal"),
    "4": ("Tomato", "Ingredient"),
    "5": ("Mozarella", "Ingredient"),
    "6": ("Basil", "Ingredient"),
    "7": ("Cardboard Box", "Packaging"),
    "8": ("Icecream", "Dessert"),
}

BASE_PIZZA_PRICE = 15
PACKAGE_PRICE = 3


def get_time(dt) -> tuple[str, int, str]:

    current_time = "night"
    weekday = dt.weekday()
    for time, config in TIME_CONFIGS.items():
        if is_time_in_range(config["start"], config["end"], dt.time()):
            current_time = time
            break
    seed = dt.strftime(f"%j %H")
    return current_time, weekday, seed


def is_time_in_range(start_time: time, end_time: time, current_time: time):

    if start_time <= end_time:
        return start_time <= current_time <= end_time
    else:
        return current_time >= start_time or current_time <= end_time


def get_pizza_lam(current_time: str, weekday: int) -> int:
    if current_time == "lunch":
        lam = 15
    elif current_time == "afternoon":
        lam = 3
    else:
        lam = 25
    if weekday >= 4:
        lam += 10
    return lam


def get_icecream_data(current_time: str, weekday: int) -> tuple[int, float]:
    # our icecream seller is fair and does not change his prices
    price = 7
    if current_time == "lunch":
        # least icecream during lunch
        lam = 3
    elif current_time == "afternoon":
        # most in the afternoon
        lam = 15
    else:
        # none in the evening
        lam = 6
    if weekday >= 5:
        # more icecream on weekdays
        lam += 2
    volume = np.random.poisson(lam=lam)
    return volume, price


def get_pizza_data(current_time: str, weekday: int, item: str) -> tuple[int, float]:
    # also our pizza seller is fair, and does not adjust his prices that often
    size_factor = 1
    if item == "Large Pizza":
        # large pizza costs more
        size_factor = 2
    elif item == "Tiny Pizza":
        # tiny pizza costs less
        size_factor = 0.6

    price = BASE_PIZZA_PRICE * size_factor + PACKAGE_PRICE
    # depending on day of time (lunch, afternoon or evening) we sell on average (20, 3, 35) special sized pizzas
    # double the amount of the regular pizzas
    lam = get_pizza_lam(current_time, weekday)
    if item == "Regular Pizza":
        lam *= 2

    volume = np.random.poisson(lam=lam)
    return volume, price


def get_ingredient_data(
    current_time: str, weekday: int, item: str
) -> tuple[int, float]:
    ingredient_factor = 1
    if item == "Tomato":
        ingredient_factor = 0.2
    elif item == "Mozarella":
        ingredient_factor = 0.4
    elif item == "Basil":
        ingredient_factor = 0.1

    # Ingreadients are a fraction of the base pizza price
    mean = BASE_PIZZA_PRICE * ingredient_factor

    # During rush-hours, our pizza guy will ask more if you want to buy his precious items
    if current_time in ["lunch", "dinner"]:
        mean *= 1.3

    # It also depends on the weekday. During the week, the number of customers
    # and thus his willingness to sell ingredients fluctuates more
    sd_factor = 0.3
    if weekday <= 4:
        sd_factor = sd_factor * 2

    sd = sd_factor * mean
    price = max(0.1, np.random.normal(loc=mean, scale=sd))
    # People prefer buying pizzas than ingredients
    lam = get_pizza_lam(current_time, weekday) // 3
    volume = np.random.poisson(lam=lam)
    return volume, price


def get_cardboard_box_data(
    current_time: str,
    weekday: int,
) -> tuple[int, float]:
    # People buy cardboard boxes even less than ingredients
    lam = get_pizza_lam(current_time, weekday) // 4
    volume = np.random.poisson(lam=lam)
    if weekday <= 4:
        # During the week, all the busy people just want to take-away
        # and sadly no one wants to sit in, increasing the price
        return volume, PACKAGE_PRICE * 1.5
    return volume, PACKAGE_PRICE


@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "API for querying your local pizza shop's prices",
        "endpoints": [
            {
                "url": "/price",
                "description": "Returns the prices for an item. Required parameter: item_id. Optional parameter: time (default: now).",
            },
            {
                "url": "/docs",
                "description": "Auto-generated docs for the API.",
            },
        ],
    }


@app.get("/price")
async def get_price(item_id: int, time: datetime = datetime.now()):
    """Returns price and volume for the specified item at the specified date"""
    current_time, weekday, seed = get_time(time)
    item, type = ITEM_CONFIG.get(str(item_id), (None, None))
    if not item:
        return {"success": False, "error": f"Item with id {item_id} not found."}
    seed += "-" + item
    if current_time == "night":
        # Shop is closed, nothing is traded
        return {
            "price": 0,
            "volume": 0,
            "success": True,
            "seed": seed,
        }

    seed = abs(hash(seed)) % (2**32)
    random.seed(seed)
    np.random.seed(seed)
    print(f"Item: {item}, Type: {type}, Seed: {seed}")
    if item == "Icecream":
        volume, price = get_icecream_data(
            current_time,
            weekday,
        )
    elif type == "Meal":
        volume, price = get_pizza_data(
            current_time,
            weekday,
            item,
        )
    elif type == "Ingredient":
        volume, price = get_ingredient_data(
            current_time,
            weekday,
            item,
        )
    elif type == "Packaging":
        volume, price = get_cardboard_box_data(
            current_time,
            weekday,
        )

    return {
        "price": price,
        "volume": volume,
        "success": True,
        "seed": seed,
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
