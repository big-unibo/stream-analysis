## Simulator
Main url: `137.204.74.57:49992`
API provides endpoints for generating data and sending it to the Kafka topic.

- Start dataset generation endpoint
  - URL: `/generator`
  - Method: `GET`
  - Description: `Starts the generation of the dataset in kafka.`
  - Parameters:
    - `type` (String): Type of the dataset to generate. Must be one of SYNTHETIC, BITBANG, WELASER
    - `frequency` (Long): Frequency of dataset generation in milliseconds. Must be provided. 
    - `impact` (Double, required for SYNTHETIC): Impact value for synthetic dataset generation. Must be in the range [0, 1]. 
    - `extension` (Double, required for SYNTHETIC): Extension value for synthetic dataset generation. Must be in the range [0, 1]. 
    - `changeDuration` (Long): Duration of the window of changes in milliseconds. Must be provided for SYNTETIC.
  - Response: 
      ```json
              {
                 "status": status_code,
                 "message": Message detailing the issue.
              }
      ```
      If the status code is 200 then the dataset generation has been started successfully.
  - example of a request: 
    - `137.204.74.57:49992/generator?type=WELASER&frequency=10`
    - `137.204.74.57:49992/generator?type=SYNTHETIC&frequency=10&changeDuration=1000000&impact=0.5&extension=0.3`
- Stop dataset generation endpoint
  - URL: `/stop`
  - Method: `GET`
  - Description: `Stops the generation of the dataset in kafka.`
  - Response (same hase the start end point)
  - example of a request: `137.204.74.57:49992/stop`

## Algorithm
Main url: `137.204.74.57:49991`
API provides endpoints for starting and stopping an algorithm simulation. 

- Start algorithm endpoint
  - URL: `/algorithm`
  - Method: `GET`
  - Description: `Starts the algorithm computation with the specified parameters.`
  - Parameters:
    - `windowDuration` (Long): Duration of the window in milliseconds. Must be greater than 0.
    - `slideDuration` (Long): Duration of the slide in milliseconds. Must be greater than 0.
    - `k` (Int): Number of attributes to group by. Must be greater than 0.
    - `alpha` (Double): Alpha value for the algorithm. Must be in the range [0, 1].
    - `stateCapacity` (Double): Percentage of records in state. Must be in the range (0, 1].
  - Response: 
      ```json
              {
                 "status": status_code,
                 "message": Message detailing the issue.
              }
      ```
      If the status code is 200 then the algorithm has been started successfully.
  - example of a request: `137.204.74.57:49991/algorithm?slideDuration=100000&windowDuration=1000000&alpha=0.5&k=2&stateCapacity=0.05`
       
- Stop algorithm endpoint
  - URL: `/stop`
  - Method: `GET`
  - Description: `Stops the algorithm computation.`
  - Response (same hase the start end point)
  - example of a request: `137.204.74.57:49991/stop`

## Kafka response documentation
- broker url: `137.204.74.57:49092`
- topic: `output_stream_analysis`
  - message format:
    ```json
    {
       "result": results,
       "dimensions-support": {"dim_1": supp_1,"dim_2": supp_2, ...,"dim_n": supp_n},
       "paneSize": number_of_input_records_for_the_current_window_pane,
       "paneTime": pane_timestamp
    }
    ```
    - results is a csv comma separated, first line is the header
    - supp is the support of the dimension, the value is in the range [0, 1]
    - example of a message:
      ```json
      {"result": "farm_creation_date_year,status,Avg(fuel),Min(batteryLevel),Avg(heartbeat),Avg(batteryLevel),Sum(heartbeat),Max(Speed),Min(Speed),Sum(batteryLevel),Count(Speed),Count(heartbeat),Max(batteryLevel),Sum(Speed),Count(fuel),Max(fuel),Count(batteryLevel),Avg(Speed),Max(heartbeat),Sum(fuel),Min(fuel),Min(heartbeat)
      farm_creation_date_year-2,status-2,100.00020856003145,99.71928002843934,99.99338995668722,99.99630487451118,31597.91122631316,50.250921337511954,49.721039326580836,31598.832340345532,316.0,316.0,100.26611769993869,15800.01667194739,316.0,100.27296363420373,316.0,50.00005275932718,100.26168852078959,31600.06590496994,99.778727819806,99.689894950841
      farm_creation_date_year-3,status-1,99.98869377195982,99.71981638460247,100.00213205940594,100.0000601687066,29200.622561346536,50.26829943169005,49.72954537696127,29200.017569262327,292.0,292.0,100.29137541533483,14597.83806030891,292.0,100.2684273169103,292.0,49.99259609694832,100.29127428223323,29196.698581412267,99.75192060842855,99.73170762479378
      farm_creation_date_year-1,status-1,100.0099833432075,99.60856706603425,99.99655109256959,99.99885050195164,26999.06879499379,50.356725336008466,49.76287549350892,26999.689635526942,270.0,270.0,100.25833010459321,13503.38848960746,270.0,100.28561952140463,270.0,50.01254996150911,100.25939423867135,27002.695502666025,99.76117499184532,99.57755464014042
      farm_creation_date_year-3,status-2,100.0090050420942,99.73711120058347,99.99493160279508,100.00724044644228,24798.74303749318,50.299839542573174,49.757500836715856,24801.795630717686,248.0,248.0,100.26828654562564,12398.662182042175,248.0,100.31415493703568,248.0,49.99460557275071,100.21687453849283,24802.233250439363,99.69693474082531,99.72482234142603
      farm_creation_date_year-1,status-2,100.00485535731791,99.70883427704695,99.99610708589562,99.9977172957855,25499.007306903382,50.241164720228376,49.777112726377474,25499.417910425305,255.0,255.0,100.29912549380023,12750.834502393656,255.0,100.30369074268987,255.0,50.00327255840649,100.32115828130492,25501.238116116067,99.63837567136066,99.69864003285436
      farm_creation_date_year-2,status-1,99.9912161334443,99.75076465308668,100.00569813767166,100.00902320473737,28601.629667374094,50.2990693517668,49.68951178806893,28602.58063655489,286.0,286.0,100.2712457444933,14297.992802724686,286.0,100.2638049517497,286.0,49.99298182770869,100.27881217919901,28597.48781416507,99.7052744995947,99.73644235538232",
      "dimensions-support": {"area": 1.0,"activity_timestamp_day": 1.0,"task": 1.0,"model": 0.8,"farm_creation_date_year": 1.0,"activity_timestamp_min": 1.0,"activity_timestamp_month": 1.0,"goal": 0.698,"country": 0.287,"activity_timestamp_hour": 1.0,"company": 1.0,"activity_timestamp_year": 1.0,"status": 1.0,"farm_creation_date_month": 1.0,"robot": 0.562,"region": 0.805,"activity_timestamp_s": 1.0,"farm": 1.0,"locality": 1.0,"farm_creation_date_day": 1.0,"activity_timestamp_ms": 1.0},
      "paneSize": 1000, "paneTime": 1721205362165}
      ```