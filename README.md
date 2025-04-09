# Product Dedupe debug and explanation logic

# As a first step, I've inspected and filtered the main database so I can be able to have a better global overview of it. After that, I started to look for similarities inside it. Even though many entries are pretty similar, there are key attributes that differentiates them, which made the task a little challenging. While scrolling through the entries, I managed to find out that one of the main attributes that will help me out in the deduplication process will be root_domain because there can be many listed products that can have the same root_domain as common attribute and that will be helpful for the process. Another attribute that I found out that is common for many entries is product_name. Same for root_domain, you can have inputs that have similar name, but slightly different descriptions inside the other columns and that would work out for the deduplication process as well. But that wouldn't be enough, since I found out that many strings need data clean-up, so that the overview would be the correct one in order to correctly proceed with the dedupe. 

# As for the main concern of this subject, which is string similarity, the function that I chose for this matter is Levenshtein distance, which will help me find out the differences between strings.

## So, as a first step, having a few key attribues as a start-off and the idea of doing a database clean-up, I proceeded to look for ideas that'll help me out along the way.

# Having those inputs in mind, I begin to debug the file, as follows:

#  -> I started to declare the libraries that I'll use for the deduplication process
#  -> In terms of strings clean-up, I chose string normalization function in order to make them standard
#  -> After that, I've created the Spark session and loaded the document
#  -> Continuing, I've declarred the Levenshtein distance function for similar strings
#  -> Moving on, I've defined the functions that'll return the similarity between strings and identify the column's type
#  -> After that, I've declared the Pandas, Cluster DataFrame, the new row for deduplicated values and the actual hash-objects deduplication
#  -> Afterwards, the repartition of the DF has begun, then the deduplincation
#  -> In the end, there's the final deduplication process, the results overview and Parquet delivery

# Thanks!
