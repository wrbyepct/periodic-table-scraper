import scrapy
from scrapy_playwright.page import PageMethod
from ptable.items import PtableItem
from scrapy.loader import ItemLoader


class PtablespiderSpider(scrapy.Spider):
    name = "ptablespider"
    allowed_domains = ["pubchem.ncbi.nlm.nih.gov"]
    start_urls = ["https://pubchem.ncbi.nlm.nih.gov/ptable/"]
    
    def start_requests(self):
        yield scrapy.Request(
            url=self.start_urls[0],
            meta=dict(
                playwright=True,
                playwright_page_methods=[
                    PageMethod('wait_for_selector', 'div[class*="ptabl"][role="list"]')
                ]
            )
        )
    
    
    async def parse(self, response):
    
        ## TODO  item loader 
        atoms = response.css('div[role="listitem"]')
        for atom in atoms:
            item = ItemLoader(item=PtableItem(), selector=atom)

            item.add_css('symbol', '[data-tooltip="Symbol"]')
            item.add_css('name', '[data-tooltip="Name"]')
            item.add_css('atomic_number', '[data-tooltip="Atomic Number"]')
            item.add_css('atomic_mass', '[data-tooltip*="Atomic Mass"]')
            item.add_css('chemical_group', '[data-tooltip="Chemical Group Block"]')

            yield item.load_item()
    
            
