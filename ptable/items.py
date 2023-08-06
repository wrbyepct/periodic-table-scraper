import scrapy
from itemloaders.processors import TakeFirst, MapCompose
from w3lib.html import remove_tags

class PtableItem(scrapy.Item):
    symbol = scrapy.Field(
        input_processor=MapCompose(remove_tags, str.strip),
        output_processor=TakeFirst()
    )

    name = scrapy.Field(
        input_processor=MapCompose(remove_tags, str.strip),
        output_processor=TakeFirst()
    )

    atomic_number = scrapy.Field(
        input_processor=MapCompose(remove_tags, str.strip, lambda x: int(x)),
        output_processor=TakeFirst()
    )

    atomic_mass = scrapy.Field(
        input_processor=MapCompose(remove_tags, str.strip, lambda x: float(x)),
        output_processor=TakeFirst()
    )

    chemical_group = scrapy.Field(
        input_processor=MapCompose(remove_tags, str.strip),
        output_processor=TakeFirst()
    )
