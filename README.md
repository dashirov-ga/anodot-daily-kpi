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
                                                                                  
                                                                                  
#### KPIs
* Team KPIs 
  
  Blind copy of key performance metrics as outlined by GA teams and persisted in KPI Repository
   
* New GA Visitors
* New To File
* CORE Leads
* Admissions Calls 
(_Admissions Calls Made_,_Admissions Calls Connected_)
