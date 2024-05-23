from datetime import datetime
  
def validate_populate_table_params(num_of_users,num_of_hr_records,age_to,age_from):
    """
    Validating user input before executing the query
    """

    if not isinstance(num_of_users,int):
        raise ValueError("user_id must be an integer")
    if not isinstance(num_of_hr_records,int):
        raise ValueError("num_of_hr_records must be an integer")
    if not isinstance(age_to,int):
        raise ValueError("min_age must be an integer")
    if not isinstance(age_from,int):
        raise ValueError("min_age must be an integer")
    if age_to >= age_from:
        raise ValueError("age_from cannot be greater that 'age_from'")
    
def validate_query_users_params(min_age, gender, min_avg_heart_rate, date_from, date_to):
    """
    Validating user input before executing the query
    """

    if not isinstance(min_age,int):
        raise ValueError("min_age must be an integer")
    if gender.upper() not in ['M', 'F']:
        raise ValueError("gender must be 'M' or 'F'")
    if not isinstance(min_avg_heart_rate,(float, int)):
        raise ValueError("min_avg_heart_rate must be a float or integer")
    if not isinstance(date_from,datetime):
        raise ValueError("date_from must be a valid datetime string")
    if not isinstance(date_to,datetime):
        raise ValueError("date_to must be a valid datetime string")


def validate_query_for_user_params(user_id, date_from, date_to):
    """
    Validating user input before executing the query
    """
    
    if not isinstance(user_id,int):
        raise ValueError("min_age must be an integer")
    if not isinstance(date_from,datetime):
        raise ValueError("date_from must be a valid datetime string")
    if not isinstance(date_to,datetime):
        raise ValueError("date_to must be a valid datetime string")  



