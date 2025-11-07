"""Mock data for drinks."""

from custom_types.drink import DrinkCategory, DrinkV2, DrinkVolume

categories: list[DrinkCategory] = [
    DrinkCategory(
        id=0,
        name="All",
        icon="../category-icons/reshot-icon-beer-box-M2X4LB63YW.svg",
        compound_category=True,
    ),
    DrinkCategory(id=1, name="Beer", icon="../category-icons/reshot-icon-beer-mug-JCKMP6FS5Z.svg"),
    DrinkCategory(id=2, name="Soda", icon="../category-icons/reshot-icon-takeaway-drink-G5L72FVUTX.svg"),
    DrinkCategory(id=3, name="Coffee", icon="../category-icons/reshot-icon-hot-drink-XSCEQUDB86.svg"),
]


drinks: list[DrinkV2] = [
    DrinkV2(id=0, name="Crema", category=categories[3], enabled=True, image="../coffee-white-bg.png"),
    DrinkV2(id=1, name="Espresso", category=categories[3], enabled=True, image="../coffee-white-bg.png"),
    DrinkV2(
        id=2,
        name="Sparkling water",
        category=categories[2],
        enabled=True,
        image="../big-water.jpg",
        volumes=[
            DrinkVolume(id=1, name="small", image="../small-sprite.png"),
            DrinkVolume(id=2, name="large", image="../big-water.jpg"),
        ],
        brand_image="../logos/bubbles-svgrepo-com.svg",
    ),
    DrinkV2(id=3, name="Doppio", category=categories[3], enabled=True, image="../cappuccino-white-bg.png"),
    DrinkV2(
        id=4,
        name="Coca-Cola",
        category=categories[2],
        enabled=True,
        image="../big-coke.png",
        volumes=[
            DrinkVolume(id=1, name="small", image="../small-coke.png"),
            DrinkVolume(id=2, name="large", image="../big-coke.png"),
        ],
        brand_image="../logos/coca-cola.svg",
    ),
    DrinkV2(
        id=5,
        name="Fanta",
        category=categories[2],
        enabled=True,
        image="../big-fanta.png",
        volumes=[
            DrinkVolume(id=1, name="small", image="../small-fanta.png"),
            DrinkVolume(id=2, name="large", image="../big-fanta.png"),
        ],
        brand_image="../logos/Fanta_2023.svg",
    ),
    DrinkV2(
        id=6,
        name="Still Water",
        category=categories[2],
        enabled=True,
        image="../big-water.jpg",
        volumes=[
            DrinkVolume(id=1, name="small", image="../small-sprite.png"),
            DrinkVolume(id=2, name="large", image="../big-water.jpg"),
        ],
    ),
    DrinkV2(
        id=7,
        name="Tonic",
        category=categories[2],
        enabled=True,
        image="../big-sprite.png",
        volumes=[
            DrinkVolume(id=1, name="small", image="../small-sprite.png"),
            DrinkVolume(id=2, name="large", image="../big-sprite.png"),
        ],
        brand_image="../logos/KINLEY_LOGO_2023_K.png",
    ),
    DrinkV2(
        id=8,
        name="Sprite",
        category=categories[2],
        enabled=True,
        image="../big-sprite.png",
        volumes=[
            DrinkVolume(id=1, name="small", image="../small-sprite.png"),
            DrinkVolume(id=2, name="large", image="../big-sprite.png"),
        ],
        brand_image="../logos/Sprite_2022.svg",
    ),
    DrinkV2(id=10, name="Cappuccino", category=categories[3], enabled=True, image="../cappuccino-white-bg.png"),
    DrinkV2(id=11, name="Latte Macchiato", category=categories[3], enabled=True, image="../cappuccino-white-bg.png"),
    DrinkV2(id=12, name="Caffe Latte", category=categories[3], enabled=True, image="../cappuccino-white-bg.png"),
    DrinkV2(id=13, name="Ristretto", category=categories[3], enabled=True, image="../coffee-white-bg.png"),
    DrinkV2(id=14, name="Flat White", category=categories[3], enabled=True, image="../cappuccino-white-bg.png"),
    DrinkV2(id=15, name="Americano", category=categories[3], enabled=True, image="../coffee-white-bg.png"),
    DrinkV2(
        id=16,
        name="Pilsner Urquel 12°",
        category=categories[1],
        enabled=True,
        image="../pilsner-white-bg.png",
        brand_image="../logos/pilsner_slideshow.svg",
    ),
    DrinkV2(
        id=19,
        name="Radegast 0%",
        category=categories[1],
        enabled=True,
        image="../pilsner-white-bg.png",
        brand_image="../logos/birell-logo.png",
    ),
]
