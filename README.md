Examples of using [Apache Spark](http://spark.apache.org/) to quickly answer questions about a dataset.

In this case, the dataset is a subset of the Enron email corpus ([download](http://bailando.sims.berkeley.edu/enron/enron_with_categories.tar.gz)).

This project is set up to use [Maven](https://maven.apache.org/install.html) to download dependencies including Scala
and Apache Spark. Make sure the email data is placed at src/main/resources/enron_with_categories.

With Maven installed, `mvn compile` to download dependencies. Then run the tests.
