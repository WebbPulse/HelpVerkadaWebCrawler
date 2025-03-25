from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from HelpVerkadaWebCrawler.spiders.KbSpider import KbSpider
import os
import pandas as pd

def find_links_to_specified_page():
    # Check if data is present
    if not check_for_data():
        return
    choice = input("Enter the URL of the page you want to find links to: ").strip()
    # Read the Edges CSV file
    df = pd.read_csv('./Output/Edges.csv')

    # Filter rows and select only 'Source' column
    filtered_df = df[df['Target'] == choice]

    # Check if any sources were found
    if filtered_df.empty:
        print(f"No sources found for target {choice}.")
    else:
        for index, row in filtered_df.iterrows():
            print(f"Index: {index}, Source: {row['Source']}, Target: {row['Target']}")
        
        #Ask if user wants to save to file
        export = filtered_df
        save_to_file(export)

def find_links_to_any_page_in_list():
    # Check if data is present
    if not check_for_data():
        return
    
    # Get the path to the file containaing the list of pages
    file_path = input("Enter the path to the file containing the list of pages you want to find links to: ").strip()

    # Read the list of pages from the file
    with open(f"{file_path}", 'r') as file:
        pages_list = [line.strip() for line in file]

    # Read the Edges CSV file
    df = pd.read_csv('./Output/Edges.csv')

    # Filter rows where 'Target' is in the pages list
    filtered_df = df[df['Target'].isin(pages_list)]
    
    print("Pages linking to the specified targets:")

    for index, row in filtered_df.iterrows():
        print(f"Index: {index}, Source: {row['Source']}, Target: {row['Target']}")
    
    #Ask if user wants to save to file
    export=filtered_df
    save_to_file(export)

def list_broken_links():
    if not os.path.exists('./Output/pages_not_found.csv'):
        print("No broken links file. Either the data has not been refreshed or there are no broken links.")
        return
    
    # Read the broken links CSV file
    df = pd.read_csv('./Output/pages_not_found.csv')

    filtered_df = df.drop_duplicates(subset=['URL'])
    output = filtered_df[['URL']]
        
    if output.empty:
        print(f"No broken links found.")
    else:
        for index, row in filtered_df.iterrows():
            print(f"{row['URL']}")
    
    #Ask if user wants to save to file
    export = output
    save_to_file(export)

def query_broken_links():
    if not os.path.exists('./Output/pages_not_found.csv'):
        print("No broken links file. Either the data has not been refreshed or there are no broken links.")
        return
    
    choice = input("Enter the URL of the page you want to find references for: ").strip()

    # Read the broken links CSV file
    df = pd.read_csv('./Output/pages_not_found.csv')

    filtered_df = df[df['URL'] == choice]

    # Check if any sources were found
    if filtered_df.empty:
        print(f"No references found for target {choice}.")
    else:
        for index, row in filtered_df.iterrows():
            print(f"Index: {index}, URL: {row['URL']}, Referring URL: {row['Referring URL']}")
        
        #Ask if user wants to save to file
        export = filtered_df
        save_to_file(export)

def list_permanent_redirects():
    if not os.path.exists('./Output/permanent_redirects.csv'):
        print("No redirects file. Either the data has not been refreshed or there are no redirects.")
        return
    
    # Read the broken links CSV file
    df = pd.read_csv('./Output/permanent_redirects.csv')

    filtered_df = df.drop_duplicates(subset=['URL'])
    output = filtered_df[['URL']]
    
    if output.empty:
        print(f"No redirects found.")
    else:
        for index, row in filtered_df.iterrows():
            print(f"{row['URL']}")
    
    #Ask if user wants to save to file
    export = output
    save_to_file(export)

def query_permanent_redirects():
    if not os.path.exists('./Output/permanent_redirects.csv'):
        print("No redirect file. Either the data has not been refreshed or there are no redirects.")
        return
    
    choice = input("Enter the URL of the page you want to find references for: ").strip()

    # Read the redirects CSV file
    df = pd.read_csv('./Output/permanent_redirects.csv')

    filtered_df = df[df['URL'] == choice]

    # Check if any sources were found
    if filtered_df.empty:
        print(f"No references found for target {choice}.")
    else:
        for index, row in filtered_df.iterrows():
            print(f"Index: {index}, URL: {row['URL']}, Referring URL: {row['Referring URL']}")
        
        #Ask if user wants to save to file
        export = filtered_df
        save_to_file(export)

def refresh_data():

    print("Refreshing data...")
    
    #Remove any existing files bnefore the refresh while not touching the git files

    if os.path.exists('./Output/pages_not_found.csv'):
        os.remove('./Output/pages_not_found.csv')
    if os.path.exists('./Output/permanent_redirects.csv'):
        os.remove('./Output/permanent_redirects.csv')
    if os.path.exists('./Output/Nodes.csv'):
        os.remove('./Output/Nodes.csv')
    if os.path.exists('./Output/Edges.csv'):
        os.remove('./Output/Edges.csv')
    if os.path.exists('./Output/KbSpider.log'):
        os.remove('./Output/KbSpider.log')
    
    #rerun web scraper

    process = CrawlerProcess(get_project_settings())
    process.crawl(KbSpider)
    process.start()

    print("Data refreshed.")

def check_for_data():
    base_path = './Output'
    files_to_check = ['Nodes.csv', 'Edges.csv']
    missing_files = [file for file in files_to_check if not os.path.exists(os.path.join(base_path, file))]

    if missing_files:
        print("Please refresh data first. Missing files: " + ", ".join(missing_files))
        return False
    else:
        print("Scraped data present.")
        return True

def save_to_file(export):
    
    while True:
        ask_to_save_to_file = input("Do you want to save the output to a file? (y/n): ").strip()

        if ask_to_save_to_file == 'y':
            file_name = input("Enter the desired name of the output file (without extension): ").strip()
            export.to_csv(f'./Output/{file_name}.csv', index=False)
            print(f"Output saved to ./Output/{file_name}.csv")
            break
        elif ask_to_save_to_file == 'n':
            print("Output not saved.")
            break
        else:
            print("Invalid choice, please try again.")

if __name__ == '__main__':

    while True:
            
        print("Please select an option:")
        print("--------------------------------------------------")
        print("Page Tools:")
        print("1: Find pages that link to a specific page")
        print("2: Find pages that link to any page in a list of pages (specify a .txt file with one URL per line)")
        print("Broken Links Tools:")
        print("3: List all broken links")
        print("4: Find referances to a specific broken link")
        print("Permanent Redirects Tools:")
        print("5: List all permanent redirects")
        print("6: Find referances to a specific permanent redirect")
        print("Miscellaneous:")
        print("7: Refresh KB data")
        print("8: Exit")
    
        choice = input("Enter your choice (1/2/3/4/5/6/7/8): ").strip()


        
        if choice == '1':
            # Find links to a specific page
            find_links_to_specified_page()
            continue 
        
        elif choice == '2':
            # Find links to any page in a list of pages
            find_links_to_any_page_in_list()
            continue
        
        elif choice == '3':
            # List broken links
            list_broken_links()
            continue
        elif choice == '4':
            # query broken links
            query_broken_links()
            continue

        elif choice == '5':
            # List permanent redirects
            list_permanent_redirects()
            continue

        elif choice == '6':
            # query permanent redirects
            query_permanent_redirects()
            continue
        
        elif choice == '7':
            # Refresh web scraping data
            refresh_data()
            continue          

        elif choice == '8':
            print("Exiting the program.")
            break
        
        else:
            print("Invalid choice, please try again.")
    