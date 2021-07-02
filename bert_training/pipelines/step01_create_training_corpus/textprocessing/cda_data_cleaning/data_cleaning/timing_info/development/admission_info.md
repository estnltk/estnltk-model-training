# Description of admission\_info

| Column name | Meaning | Example |
|:-------|:---|:---|
| pat\_id               | patient ID           | ***                 |
| epi\_id               | epicrisis ID         | ***                 |
| admission\_time       | admission time       | 2012-04-25 03:30:00 |
| admission\_time\_unit | admission time unit  | second              |
| tto\_registry\_code   | official TTO code    | 90004585            |
| tto\_name             | official TTO name    | SA Viljandi Haigla  |
| treatment\_type       | treatment type       | S                   |
| admission\_code       | admission code       | 1                   |
| admission\_type       | admission type       | kiirabiga           |

* Admission time is recorded with the precision defined in `admission\_time\_unit`.
* Hospitals and other health service providers have special registry codes.
* Treatment type specifies whether patient was hospitalised or had ambulatory treatment.
* Admission type is a human readable description of the admission code.   
