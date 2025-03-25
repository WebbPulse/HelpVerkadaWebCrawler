# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import csv
import os
import pandas as pd
import time

class HelpVerkadaWebCrawlerPipeline:
    def __init__(self):

        os.makedirs("./Output", exist_ok=True)
        #main
        self.nodelist_csv_file = open('./Output/Nodelist.csv', 'w', newline='', encoding='utf-8')
        self.nodelist_csv_writer = csv.DictWriter(self.nodelist_csv_file, fieldnames=['Id', 'Label', 'Language'])
        self.nodelist_csv_writer.writeheader()

        self.collections_csv_file = open('./Output/Collections.csv', 'w', newline='', encoding='utf-8')
        self.collections_csv_writer = csv.DictWriter(self.collections_csv_file, fieldnames=['Id', 'Collection Names', 'Collection Links'])
        self.collections_csv_writer.writeheader()
        
        self.edge_csv_file = open('./Output/Edges.csv', 'w', newline='', encoding='utf-8')
        self.edge_csv_writer = csv.DictWriter(self.edge_csv_file, fieldnames=['Source', 'Target'])
        self.edge_csv_writer.writeheader()
        
    def process_item(self, item, spider):

        # Write to appropriate CSV files
        if 'Label' in item:
            print(f'Writing {item} to nodelist')
            self.nodelist_csv_writer.writerow(item)

        elif 'Collection Names' in item:
            print(f'Writing {item} to collections')
            self.collections_csv_writer.writerow(item)


        elif 'Source' in item:
            print(f'Writing {item} to edges')
            self.edge_csv_writer.writerow(item)
        
        else:
            print(f'Item does not have a valid key: {item}')

        return item



    def close_spider(self, spider):
        self.nodelist_csv_file.close()
        self.collections_csv_file.close()
        self.edge_csv_file.close()

        #CSV merging
        #sets dataframes for the two csv files
        df1 = pd.read_csv('./Output/Nodelist.csv')
        df2 = pd.read_csv('./Output/Collections.csv')

        #aggregate the collection names and urls into lists
        aggregated_df = df2.groupby('Id').agg({
            'Collection Names': list,
            'Collection Links': list
        }).reset_index()

        #merges the two dataframes and exports to csv
        result = pd.merge(df1, aggregated_df, on='Id', how='outer')
        result.to_csv('./Output/Nodes.csv', index=False)
        
        #deletes the temporary csv files
        time.sleep(3)
        if os.path.exists('./Output/Nodelist.csv'):
            os.remove('./Output/Nodelist.csv')
        if os.path.exists('./Output/Collections.csv'): 
            os.remove('./Output/Collections.csv')