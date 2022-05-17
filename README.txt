Analysis of Japanese Restaurants in LA From Yelp, Tripadvisor, OpenTable with Python


Before running the program, please install the following:
* For the purpose of downloading DataFrame as image
$ pip install DataFrame-image
* For the purpose of using Textblob for sentiment analysis
$ pip install -U textblob
$ python -m textblob.download_corpora
It is also available as a conda package. To install with conda, run
$ conda install -c conda-forge textblob
$ python -m textblob.download_corpora

There is one script:
* “FinalProject.py” to 
   * Scrape data:
      * scrape 200 Japanese restaurants info sorted by rating on Yelp website using Yelp Fusion API; 
      * scrape the best 120 Japanese restaurants info on Tripadvisor website; 
      * scrape 100 Japanese restaurants info sorted by rating on OpenTable website.
   * Merge and store data into database (csv and sqlite)
   * Analyze data
      * calculate weighted rating and sum of review count for each place
      * find most repeated word in review for each place
      * obtain sentiment rating for each place
      * sort restaurants by weighted average rating and get top 10 LA Japanese restaurants table
      * investigate the correlation between rating from each source and the correlation between weighted rating from platform and weighted rating from sentiment analysis.


The python scripts and functions within are commented. If you have a slow connection and encounter errors, try running the script again or increasing/adding time.sleep() function. Please note that the OpenTable website may not always respond with real content, trying to run the script a few times will do.


How to use:
* download the file and launch it directly from the terminal with two ways
   * FinalProject.py 
- scrape the complete data, store it in a database, then perform analysis on the database (most recently scraped data).
   * FinalProject.py --static
-open the stored database and performs analysis on stored data


Description of Maintainability/Extensibility of Model
* Maintainability: 
   * All the scripts and functions are commented and each attribute has its own session. The scraped data is first stored in a DataFrame (each attribute stored as a column in the DataFrame) and then converted into a csv file. As a result, others can make corrections and minor improvements fairly easily. And the program can be adapted fairly easily if the input data is updated/has small changes as one can adjust the corresponding session of the script.
* Extensibility:
   * The scraped data is first stored in a DataFrame and then converted and saved as a csv file. As a result, if new capabilities/functionality is needed (i.e. obtain whether the place is open to booking), one can simply add a column in the DataFrame and store the data gathered by the new functionality. In the analysis session, if one wishes to analyze some more, one can write a new function and call the function in function organize_analysis().
   * If one wants to analyze restaurants other than Japanese, one can change the url input in the scraping functions (for scraping OpenTable TripAdvisor) and adjust the "term" in parameters (for scraping Yelp).