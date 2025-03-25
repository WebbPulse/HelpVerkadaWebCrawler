# HelpVerkadaWebCrawler
 
General usage guide:

1. Set Your working directory to the directory that contains this README
2. Run run_me.py
3. If this is the first time you are running the script recently, select the "Refresh KB Data" option to rescrape data from the help.verkada.com domain
4. Use additional options in the run_me.py menu to answer questions about the data selected

Considersations:

1. Language setting is stored in settings.py in the HelpVerkadaWebCrawler directory. In the event that new languages are supported, this will need to be updates to include the prefix for the new language so that the right start URLs are used

2. All Scraped Data and custom query CSVs are stored in the Output folder. This folders contents are not synced with github
