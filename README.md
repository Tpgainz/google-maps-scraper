# Google maps scraper

### Command line:

```
touch results.csv && docker run -v $PWD/example-queries.txt:/example-queries -v $PWD/results.csv:/results.csv gosom/google-maps-scraper -depth 1 -input /example-queries -results /results.csv -exit-on-inactivity 3m
```

file `results.csv` will contain the parsed results.

\*_If you want emails use additionally the `-email` parameter_

### REST API

The Google Maps Scraper provides a RESTful API for programmatic management of scraping tasks.

### Key Endpoints

- POST /api/v1/jobs: Create a new scraping job
- GET /api/v1/jobs: List all jobs
- GET /api/v1/jobs/{id}: Get details of a specific job
- DELETE /api/v1/jobs/{id}: Delete a job
- GET /api/v1/jobs/{id}/download: Download job results as CSV

For detailed API documentation, refer to the OpenAPI 3.0.3 specification available through Swagger UI or Redoc when running the app https://localhost:8080/api/docs

## 🌟 Support the Project!

If you find this tool useful, consider giving it a **star** on GitHub.
Feel free to check out the **Sponsor** button on this repository to see how you can further support the development of this project.
Your support helps ensure continued improvement and maintenance.

## Features

- Extracts many data points from google maps
- Exports the data to CSV, JSON or PostgreSQL
- Performance about 120 urls per minute (-depth 1 -c 8)
- Extendable to write your own exporter
- Dockerized for easy run in multiple platforms
- Scalable in multiple machines
- Optionally extracts emails from the website of the business
- SOCKS5/HTTP/HTTPS proxy support
- Serverless execution via AWS Lambda functions (experimental & no documentation yet)
- Fast Mode (BETA)

## Notes on email extraction

By default email extraction is disabled.

If you enable email extraction (see quickstart) then the scraper will visit the
website of the business (if exists) and it will try to extract the emails from the
page.

For the moment it only checks only one page of the website (the one that is registered in Gmaps). At some point, it will be added support to try to extract from other pages like about, contact, impressum etc.

Keep in mind that enabling email extraction results to larger processing time, since more
pages are scraped.

## Fast Mode

Fast mode returns you at most 21 search results per query ordered by distance from the **latitude** and **longitude** provided.
All the results are within the specified **radius**

It does not contain all the data points but basic ones.
However it provides the ability to extract data really fast.

When you use the fast mode ensure that you have provided:

- zoom
- radius (in meters)
- latitude
- longitude

**Fast mode is Beta, you may experience blocking**

## Extracted Data Points

#### 1. `input_id`

- Internal identifier for the input query.

#### 2. `link`

- Direct URL to the business listing on Google Maps.

#### 3. `title`

- Name of the business.

#### 4. `category`

- Business type or category (e.g., Restaurant, Hotel).

#### 5. `address`

- Street address of the business.

#### 6. `open_hours`

- Business operating hours.

#### 7. `popular_times`

- Estimated visitor traffic at different times of the day.

#### 8. `website`

- Official business website.

#### 9. `phone`

- Business contact phone number.

#### 10. `plus_code`

- Shortcode representing the precise location of the business.

#### 11. `review_count`

- Total number of customer reviews.

#### 12. `review_rating`

- Average star rating based on reviews.

#### 13. `reviews_per_rating`

- Breakdown of reviews by each star rating (e.g., number of 5-star, 4-star reviews).

#### 14. `latitude`

- Latitude coordinate of the business location.

#### 15. `longitude`

- Longitude coordinate of the business location.

#### 16. `cid`

- **Customer ID** (CID) used by Google Maps to uniquely identify a business listing. This ID remains stable across updates and can be used in URLs.
- **Example:** `3D3174616216150310598`

#### 17. `status`

- Business status (e.g., open, closed, temporarily closed).

#### 18. `descriptions`

- Brief description of the business.

#### 19. `reviews_link`

- Direct link to the reviews section of the business listing.

#### 20. `thumbnail`

- URL to a thumbnail image of the business.

#### 21. `timezone`

- Time zone of the business location.

#### 22. `price_range`

- Price range of the business (`$`, `$$`, `$$$`).

#### 23. `data_id`

- An internal Google Maps identifier composed of two hexadecimal values separated by a colon.
- **Structure:** `<spatial_hex>:<listing_hex>`
- **Example:** `0x3eb33fecd7dfa167:0x2c0e80a0f5d57ec6`
- **Note:** This value may change if the listing is updated and should not be used for permanent identification.

#### 24. `images`

- Links to images associated with the business.

#### 25. `reservations`

- Link to book reservations (if available).

#### 26. `order_online`

- Link to place online orders.

#### 27. `menu`

- Link to the menu (for applicable businesses).

#### 28. `owner`

- Indicates whether the business listing is claimed by the owner.

#### 29. `complete_address`

- Fully formatted address of the business.

#### 30. `about`

- Additional information about the business.

#### 31. `user_reviews`

- Collection of customer reviews, including text, rating, and timestamp.

#### 32. `emails`

- Email addresses associated with the business, if available.

#### 33. `user_reviews_extended`

- Collection of customer reviews, including text, rating, and timestamp. This includes all the
  reviews that can be extracted (up to around 300)

**Note**: email is empty by default (see Usage)

**Note**: Input id is an ID that you can define per query. By default it's a UUID
In order to define it you can have an input file like:

**Note**: user_reviews_extended is empty by default. You need to start the program with the
`-extra-reviews` command line flag to enabled this (see Usage)

```
Matsuhisa Athens #!#MyIDentifier
```

## Quickstart

### Using docker:

```
touch results.csv && docker run -v $PWD/example-queries.txt:/example-queries -v $PWD/results.csv:/results.csv gosom/google-maps-scraper -depth 1 -input /example-queries -results /results.csv -exit-on-inactivity 3m
```

file `results.csv` will contain the parsed results.

**If you want emails use additionally the `-email` parameter**

**All Reviews**
You can fetch up to around 300 reviews instead of the first 8 by using the
command line parameter `--extra-reviews`. If you do that I recommend you use JSON
output instead of CSV.

### On your host

(tested only on Ubuntu 22.04)

**make sure you use go version 1.24.3**

```
git clone https://github.com/gosom/google-maps-scraper.git
cd google-maps-scraper
go mod download
go build
./google-maps-scraper -input example-queries.txt -results restaurants-in-cyprus.csv -exit-on-inactivity 3m
```

Be a little bit patient. In the first run it downloads required libraries.

The results are written when they arrive in the `results` file you specified

**If you want emails use additionally the `-email` parameter**

### Using a Proxy

#### UI

From the UI set the url, username and password

#### Command line

Use the `-proxies` option like:

```
./google-maps-scraper -input example-queries.txt -results random.txt -proxies '<proxy1>,<proxy2>' -depth 1 -c 2
```

where `<proxy1>,...<proxyN>` is a valid proxy url like:

```
'scheme://username:password@host:port
```

if your proxy does not require authentication:

```
scheme://host:port
```

Supported schemes:

- socks5
- socks5h
- http
- https

I encourange you to buy a proxy service from one of our sponsors.
They are reliable and help me to maintain the project.

### Command line options

try `./google-maps-scraper -h` to see the command line options available:

```
  -addr string
        address to listen on for web server (default ":8080")
  -aws-access-key string
        AWS access key
  -aws-lambda
        run as AWS Lambda function
  -aws-lambda-chunk-size int
        AWS Lambda chunk size (default 100)
  -aws-lambda-invoker
        run as AWS Lambda invoker
  -aws-region string
        AWS region
  -aws-secret-key string
        AWS secret key
  -c int
        sets the concurrency [default: half of CPU cores] (default 1)
  -cache string
        sets the cache directory [no effect at the moment] (default "cache")
  -data-folder string
        data folder for web runner (default "webdata")
  -debug
        enable headful crawl (opens browser window) [default: false]
  -depth int
        maximum scroll depth in search results [default: 10] (default 10)
  -disable-page-reuse
        disable page reuse in playwright
  -dsn string
        database connection string [only valid with database provider]
  -email
        extract emails from websites
  -exit-on-inactivity duration
        exit after inactivity duration (e.g., '5m')
  -extra-reviews
        enable extra reviews collection
  -fast-mode
        fast mode (reduced data collection)
  -function-name string
        AWS Lambda function name
  -geo string
        set geo coordinates for search (e.g., '37.7749,-122.4194')
  -input string
        path to the input file with queries (one per line) [default: empty]
  -json
        produce JSON output instead of CSV
  -lang string
        language code for Google (e.g., 'de' for German) [default: en] (default "en")
  -produce
        produce seed jobs only (requires dsn)
  -proxies string
        comma separated list of proxies to use in the format protocol://user:pass@host:port example: socks5://localhost:9050 or http://user:pass@localhost:9050
  -radius float
        search radius in meters. Default is 10000 meters (default 10000)
  -results string
        path to the results file [default: stdout] (default "stdout")
  -s3-bucket string
        S3 bucket name
  -web
        run web server instead of crawling
  -writer string
        use custom writer plugin (format: 'dir:pluginName')
  -zoom int
        set zoom level (0-21) for search (default 15)
```

## Using a custom writer

In cases the results need to be written in a custom format or in another system like a db a message queue or basically anything the Go plugin system can be utilized.

Write a Go plugin (see an example in examples/plugins/example_writeR.go)

Compile it using (for Linux):

```
go build -buildmode=plugin -tags=plugin -o ~/mytest/plugins/example_writer.so examples/plugins/example_writer.go
```

and then run the program using the `-writer` argument.

See an example:

1. Write your plugin (use the examples/plugins/example_writer.go as a reference)
2. Build your plugin `go build -buildmode=plugin -tags=plugin -o ~/myplugins/example_writer.so plugins/example_writer.go`
3. Download the lastes [release](https://github.com/gosom/google-maps-scraper/releases/) or build the program
4. Run the program like `./google-maps-scraper -writer ~/myplugins:DummyPrinter -input example-queries.txt`

### Plugins and Docker

It is possible to use the docker image and use tha plugins.
In such case make sure that the shared library is build using a compatible GLIB version with the docker image.
otherwise you will encounter an error like:

```
/lib/x86_64-linux-gnu/libc.so.6: version `GLIBC_2.32' not found (required by /plugins/example_writer.so)
```

## Using Database Provider (postgreSQL)

For running in your local machine:

```
docker-compose -f docker-compose.dev.yaml up -d
```

The above starts a PostgreSQL container and creates the required tables

to access db:

```
psql -h localhost -U postgres -d postgres
```

Password is `postgres`

Then from your host run:

```
go run main.go -dsn "postgres://postgres:postgres@localhost:5432/postgres" -produce -input example-queries.txt --lang el
```

(configure your queries and the desired language)

This will populate the table `gmaps_jobs` .

you may run the scraper using:

```
go run main.go -c 2 -depth 1 -dsn "postgres://postgres:postgres@localhost:5432/postgres"
```

If you have a database server and several machines you can start multiple instances of the scraper as above.

### Kubernetes

You may run the scraper in a kubernetes cluster. This helps to scale it easier.

Assuming you have a kubernetes cluster and a database that is accessible from the cluster:

1. First populate the database as shown above
2. Create a deployment file `scraper.deployment`

```
apiVersion: apps/v1
kind: Deployment
metadata:
  name: google-maps-scraper
spec:
  selector:
    matchLabels:
      app: goohttps://www.scrapeless.com/gle-maps-scraper
  replicas: {NUM_OF_REPLICAS}
  template:
    metadata:
      labels:
        app: google-maps-scraper
    spec:
      containers:
      - name: google-maps-scraper
        image: gosom/google-maps-scraper:v0.9.3
        imagePullPolicy: IfNotPresent
        args: ["-c", "1", "-depth", "10", "-dsn", "postgres://{DBUSER}:{DBPASSWD@DBHOST}:{DBPORT}/{DBNAME}", "-lang", "{LANGUAGE_CODE}"]
```

Please replace the values or the command args accordingly

Note: Keep in mind that because the application starts a headless browser it requires CPU and memory.
Use an appropriate kubernetes cluster

## Environment Variables

### Docker Compose Configuration

When using Docker Compose, you can configure the following environment variables:

**INSEE API Configuration** (for French company data - recommended):

- `INSEE_API_KEY` - Your INSEE API key (Integration key: `8ad55cfb-24c6-43c1-955c-fb24c663c1cc`)

**INPI API Configuration** (alternative to INSEE):

- `INPI_USERNAME` - Your INPI e-procedures account email/username
- `INPI_PASSWORD` - Your INPI e-procedures account password
- `INPI_USE_DEMO` - Set to `true` to use the demo/preprod environment (default: `false`)

**Note**: The service automatically chains available APIs in order: INSEE → INPI → BODACC (fallback). Simply provide the credentials for the APIs you want to use. INSEE API is preferred as it's more flexible and doesn't require authentication tokens.

**Other variables**:

- `DISABLE_TELEMETRY` - Set to `1` to disable anonymous usage statistics (default: `0`)

**Example usage**:

Create a `.env` file in the project root:

```bash
# INSEE API (recommended)
INSEE_API_KEY=8ad55cfb-24c6-43c1-955c-fb24c663c1cc

# Or INPI API (alternative)
INPI_USERNAME=your-email@example.com
INPI_PASSWORD=your-password
INPI_USE_DEMO=false

# Or both - service will try INSEE first, then INPI, then BODACC

DISABLE_TELEMETRY=0
```

**With Docker Compose**:

```bash
docker-compose up
```

The environment variables will be automatically passed to the container.

**With command line**:

```bash
go run main.go -c 2 -depth 1 -dsn "postgres://postgres:postgres@localhost:5432/postgres"
```

The `.env` file will be automatically loaded at startup. If the file doesn't exist, the application will continue without it (a warning will be logged).
