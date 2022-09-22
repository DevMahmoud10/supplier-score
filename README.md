# Supplier Score

## Introduction
Hubs provides a platform where customers can upload their designs and have them manufactured
according to their desires from a wide range of technologies, materials and finishes. Behind the
scenes we work with a network of suppliers in order to source customer designs as they were
specified.

Many actions taken by our customers on the website are recorded, i.e. when they submit an order or
leave a review. All these events are stored and are used to understand how well the website is
performing (e.g., how many visitors who upload a model end up submitting an order?).

One of the many uses for this event data is to understand the quality of our supply and make
intelligent decisions about our supplier network.

Several metrics can be derived from these events, such as:
- Average review rating
- Average order acceptance rate (i.e., ratio accepted orders / total orders)

## Objective
Calculating average review and acceptance ratio for every Supplier in
the database. Each statistic should be run ans reported daily, and stored in a
```supplier_score_metrics``` table.
## How to Run
### Prerequisites
- Python3
- SQLite3
- ``` requirements.txt ```

First you need to install [python3](https://www.python.org/downloads/) and [SQLite3](https://docs.python.org/3/library/sqlite3.html)
Then run ```pip install requirements.txt``` to install any additional packages.

###
## Workflow
- Here I started in building ETL pipeline by  creating DB if not already exists then injecting dump data from ``` sql/hubs_events.sql``` to ```MY_TABLE``` in ```supply_db.sqlite```  database.
- then extracting events data from ```MY_TABLE``` and applied some preprocessing like converting ```string``` timestamp to ```datetime``` format and extract new date column plus casting string ```review_value_speed, review_value_print_quality``` to ```float```.
- After that grouped events data by ```hub_id, date``` then pass those groups to methods that will extract the target metrics.
    - ```get_average_rating``` is the first method, inside it I'm iterating over each hub_day events, filter events that contains "review" in event URL and if this hub_day had review events I calculated the average of ```review_value_speed``` and ```review_value_print_quality``` then take the average of those two numbers.
    - ```get_acceptance_ratio``` is the second one, inside it I'm iterating over each hub_day events, filter events that contains "payment" in event URL and calculate the unique accepted orders count divided by unique total orders count.
- The final step here is to merge the result of the previous methods, sort results by ```calculated_at, supplier_id```, plus loading them to ```supplier_score_metrics``` table.