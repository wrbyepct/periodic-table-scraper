# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector
import json 

class SaveToMySQLPipeline:
    def __init__(self):
        self.conn = mysql.connector.connect(
            host = '172.27.16.1',
            user = 'root',
            password = 'root',
        )

        self.cur = self.conn.cursor()

        self.cur.execute("""CREATE DATABASE IF NOT EXISTS ptable""")
        self.conn.connect(database="ptable")

    def open_spider(self, spider):
        self.cur.execute("DROP TABLE IF EXISTS ptable")

        self.cur.execute("""
            CREATE TABLE ptable (
                atomic_number INT PRIMARY KEY,
                name TEXT,
                symbol TEXT,
                atomic_mass REAL,
                chemical_group TEXT
            )
        """)
    
    def process_item(self, item, spider):

        insert_item_query = """
            INSERT INTO ptable (
                atomic_number,
                name,
                symbol,
                atomic_mass,
                chemical_group 
            ) VALUES (
                %s,
                %s,
                %s,
                %s,
                %s
            )
        """
        values = (item['atomic_number'], item['name'], item['symbol'], item['atomic_mass'], item['chemical_group'])

        self.cur.execute(insert_item_query, values)
        self.conn.commit()

        return item
    
    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()



class ChemicalGroupPipeline:
    def __init__(self):
        self.group_dict = {}
        
    def process_item(self, item, spider):
        element = dict(list(item.items())[:4]) # Only want first four key-value pairs, had to convert to list to make it subscriptable
        
        g_key = item["chemical_group"]
        if g_key in self.group_dict:
            self.group_dict[g_key]["element_count"] += 1
            self.group_dict[g_key]["elements"].append(element)
        
        else:
            g_value = {
                "element_count": 1,
                "elements": [element]
            }
            self.group_dict[g_key] = g_value
        return item 
            
    def close_spider(self, spider):
        with open('ptable.json', 'w') as f:
            json.dump(self.group_dict, f, indent=4)