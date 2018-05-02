# anodot-daily-kpi

    +------------+   ------------>  +------------------+                      +------------------+
    | RedShift   |  ------[KPI]---> | anodot-daily-kpi | ----[COUNTERS]------>| Anodot REST API* |
    +------------+   ------------>  +------------------+                      +------------------+
                                           |                                      * External SaaS Partner 
                                           |                                      (anodot.com)
                                           |                                  +------------------+
                                           +------BATCH EXECUTION STATUS----->|  Operational DB  |
                                           |                                  +------------------+
                                           |
                                           |                                  +------------------+
                                           +------BATCH EXECUTION STATUS----->| NewRelic API**   |
                                                                              +------------------+
                                                                                  ** External SaaS Partner
                                                                                  (newrelic.com)