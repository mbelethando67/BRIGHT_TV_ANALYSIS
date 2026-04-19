
WITH  Age_Group(
SELECT *,
CASE 
    WHEN Age < 18 THEN 'Kids'
    WHEN Age BETWEEN 18 AND 24 THEN 'Youth'
    WHEN Age BETWEEN 25 AND 34 THEN 'Pre Adults'
    WHEN Age BETWEEN 35 AND 44 THEN 'Adults'
    WHEN Age BETWEEN 45 AND 54 THEN 'Mature Adults'
    WHEN Age >= 55 THEN 'Pensioner'
    ELSE 'Unknown'
  END AS user_age_group,
  CASE 
    WHEN Race IS NULL OR Race = 'None' THEN 'Unknown'
    ELSE Race 
  END AS user_race
FROM `practical_work`.`default`.`BrightTV_user_profiles`)
SELECT DISTINCT UserID,
                Gender AS user_gender,
                user_age_group,
                user_race,
                Province AS user_province
  FROM Age_group
  WHERE UserID IS NOT NULL;

 
SELECT 
  UserID,
  Channel2 AS channel_name,
  
  coalesce(
    try_to_timestamp(trim(RecordDate2), 'd/M/yyyy HH:mm:ss'),
    try_to_timestamp(trim(RecordDate2), 'd/M/yyyy H:mm:ss'),
    try_to_timestamp(trim(RecordDate2), 'd/M/yyyy HH:mm'),
    try_to_timestamp(trim(RecordDate2), 'd/M/yyyy H:mm'),
    try_to_timestamp(trim(RecordDate2), 'dd/MM/yyyy HH:mm:ss'),
    try_to_timestamp(trim(RecordDate2), 'dd/MM/yyyy HH:mm')
  ) + INTERVAL 2 HOURS AS session_start_time_sa,
  
  date(
    coalesce(
      try_to_timestamp(trim(RecordDate2), 'd/M/yyyy HH:mm:ss'),
      try_to_timestamp(trim(RecordDate2), 'd/M/yyyy H:mm:ss'),
      try_to_timestamp(trim(RecordDate2), 'd/M/yyyy HH:mm'),
      try_to_timestamp(trim(RecordDate2), 'd/M/yyyy H:mm'),
      try_to_timestamp(trim(RecordDate2), 'dd/MM/yyyy HH:mm:ss'),
      try_to_timestamp(trim(RecordDate2), 'dd/MM/yyyy HH:mm')
    ) + INTERVAL 2 HOURS
  ) AS session_date_sa,
  
  hour(
    coalesce(
      try_to_timestamp(trim(RecordDate2), 'd/M/yyyy HH:mm:ss'),
      try_to_timestamp(trim(RecordDate2), 'd/M/yyyy H:mm:ss'),
      try_to_timestamp(trim(RecordDate2), 'd/M/yyyy HH:mm'),
      try_to_timestamp(trim(RecordDate2), 'd/M/yyyy H:mm'),
      try_to_timestamp(trim(RecordDate2), 'dd/MM/yyyy HH:mm:ss'),
      try_to_timestamp(trim(RecordDate2), 'dd/MM/yyyy HH:mm')
    ) + INTERVAL 2 HOURS
  ) AS session_hour_sa,
  
  date_format(
    coalesce(
      try_to_timestamp(trim(RecordDate2), 'd/M/yyyy HH:mm:ss'),
      try_to_timestamp(trim(RecordDate2), 'd/M/yyyy H:mm:ss'),
      try_to_timestamp(trim(RecordDate2), 'd/M/yyyy HH:mm'),
      try_to_timestamp(trim(RecordDate2), 'd/M/yyyy H:mm'),
      try_to_timestamp(trim(RecordDate2), 'dd/MM/yyyy HH:mm:ss'),
      try_to_timestamp(trim(RecordDate2), 'dd/MM/yyyy HH:mm')
    ) + INTERVAL 2 HOURS, 'EEEE'
  ) AS day_of_week_sa,
  
  (hour(to_timestamp(split(`Duration 2`, '\\+')[0])) * 60 + 
   minute(to_timestamp(split(`Duration 2`, '\\+')[0]))) AS duration_minutes
  
FROM `practical_work`.`default`.`bright_tv_viewership`
WHERE UserID IS NOT NULL 
  AND RecordDate2 IS NOT NULL 
  AND trim(RecordDate2)!= '';

WITH viewership_clean AS (
  SELECT 
    UserID,
    Channel2 AS channel_name,
    
    coalesce(
      try_to_timestamp(trim(RecordDate2), 'd/M/yyyy HH:mm:ss'),
      try_to_timestamp(trim(RecordDate2), 'd/M/yyyy H:mm:ss'),
      try_to_timestamp(trim(RecordDate2), 'd/M/yyyy HH:mm'),
      try_to_timestamp(trim(RecordDate2), 'd/M/yyyy H:mm'),
      try_to_timestamp(trim(RecordDate2), 'dd/MM/yyyy HH:mm:ss'),
      try_to_timestamp(trim(RecordDate2), 'dd/MM/yyyy HH:mm')
    ) + INTERVAL 2 HOURS AS session_start_time_sa,
    
    date(
      coalesce(
        try_to_timestamp(trim(RecordDate2), 'd/M/yyyy HH:mm:ss'),
        try_to_timestamp(trim(RecordDate2), 'd/M/yyyy H:mm:ss'),
        try_to_timestamp(trim(RecordDate2), 'd/M/yyyy HH:mm'),
        try_to_timestamp(trim(RecordDate2), 'd/M/yyyy H:mm'),
        try_to_timestamp(trim(RecordDate2), 'dd/MM/yyyy HH:mm:ss'),
        try_to_timestamp(trim(RecordDate2), 'dd/MM/yyyy HH:mm')
      ) + INTERVAL 2 HOURS
    ) AS session_date_sa,
    
    hour(
      coalesce(
        try_to_timestamp(trim(RecordDate2), 'd/M/yyyy HH:mm:ss'),
        try_to_timestamp(trim(RecordDate2), 'd/M/yyyy H:mm:ss'),
        try_to_timestamp(trim(RecordDate2), 'd/M/yyyy HH:mm'),
        try_to_timestamp(trim(RecordDate2), 'd/M/yyyy H:mm'),
        try_to_timestamp(trim(RecordDate2), 'dd/MM/yyyy HH:mm:ss'),
        try_to_timestamp(trim(RecordDate2), 'dd/MM/yyyy HH:mm')
      ) + INTERVAL 2 HOURS
    ) AS session_hour_sa,
    
    date_format(
      coalesce(
        try_to_timestamp(trim(RecordDate2), 'd/M/yyyy HH:mm:ss'),
        try_to_timestamp(trim(RecordDate2), 'd/M/yyyy H:mm:ss'),
        try_to_timestamp(trim(RecordDate2), 'd/M/yyyy HH:mm'),
        try_to_timestamp(trim(RecordDate2), 'd/M/yyyy H:mm'),
        try_to_timestamp(trim(RecordDate2), 'dd/MM/yyyy HH:mm:ss'),
        try_to_timestamp(trim(RecordDate2), 'dd/MM/yyyy HH:mm')
      ) + INTERVAL 2 HOURS, 'EEEE'
    ) AS day_of_week_sa,
    
    (hour(to_timestamp(split(`Duration 2`, '\\+')[0])) * 60 + 
     minute(to_timestamp(split(`Duration 2`, '\\+')[0]))) AS duration_minutes
    
  FROM `practical_work`.`default`.`bright_tv_viewership`
  WHERE UserID IS NOT NULL 
    AND RecordDate2 IS NOT NULL 
    AND trim(RecordDate2)!= ''
),
users_for_join AS (
  SELECT DISTINCT 
    UserID,
    Gender AS user_gender,
    CASE 
      WHEN Age < 18 THEN 'Kids'
      WHEN Age BETWEEN 18 AND 24 THEN 'Youth'
      WHEN Age BETWEEN 25 AND 34 THEN 'Pre Adults'
      WHEN Age BETWEEN 35 AND 44 THEN 'Adults'
      WHEN Age BETWEEN 45 AND 54 THEN 'Mature Adults'
      WHEN Age >= 55 THEN 'Pensioner'
      ELSE 'Unknown'
    END AS user_age_group,
    CASE 
      WHEN Race IS NULL OR Race = 'None' THEN 'Unknown'
      ELSE Race 
    END AS user_race,
    Province AS user_province
  FROM `practical_work`.`default`.`BrightTV_user_profiles`
  WHERE UserID IS NOT NULL
)
SELECT 
  v.*,
  u.user_gender,
  u.user_age_group, 
  u.user_province,
  u.user_race
FROM viewership_clean v
LEFT JOIN users_for_join u
  ON v.UserID = u.UserID;

