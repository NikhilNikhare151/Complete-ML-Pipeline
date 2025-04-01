import pandas as pd     # for work with DataFrame
import os               # module for making directory
from sklearn.model_selection import train_test_split        # for splitting data
import logging      # python inbiuld module      

# Ensure the "logs" direcotry exists
log_dir = "logs"        # we make sure we have a log directory where we save our all logs
os.makedirs(log_dir, exist_ok=True)     # inside os module "makedirs" is the method 
                                        # take a log_dir and exists_ok=True parameter to check log_dir is already present or not
                                        # if already present it will not create directory again skips that 
                                        # if not present then only make a directory

# Logging the configuration
logger = logging.getLogger("data_ingestion")    # making logger object of logging class
                                                # when you call logging.getlogger then you make a logger object
                                                # inside logger object you can give a name of logger object
                                                # here we give a name of logger is "data_ingestion"
logger.setLevel("DEBUG")        # here we are setting a level of logger object is "DEBUG"
                                # DEBUG means we need all information about info, warning, error and critical levels

# Console Handler
console_handler = logging.StreamHandler()       # here we are making console handler inside handler
                                                # to make this we call "StreamHandler" method inside "logging" module
console_handler.setLevel("DEBUG")       # here we are setting a level "DEBUG" for "StreamHaldler" object

# File Handler
log_file_path = os.path.join(log_dir, "data_ingestion.log") # inside "log_dir" make "data_ingestion.log" file 
file_handler = logging.FileHandler(log_file_path)   # path given to "FileHandler" for understanding of file handler where to save
file_handler.setLevel("DEBUG")  # setting level "DEBUG" for file handler

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")   # creating formatter naned variable
                                            # calling "Formatter" method inside "logging" module
                                            # "%(asctime)s - %(name)s - %(levelname)s - %(message)s" this string define
                                            # first give tine then name ("data_ingestion"), level nane then message 
console_handler.setFormatter(formatter) # formatter given to the console handler to use this format
file_handler.setFormatter(formatter)    # formatter given to the file handler to use this format

logger.addHandler(console_handler)      # putting "console_handler" inside logger object
logger.addHandler(file_handler)         # putting "file_handler" inside logger object

# Data Load function
def load_data(data_url:str)   ->  pd.DataFrame: # takes url of data
    """Load Data from csv file"""
    try:
        df = pd.read_csv(data_url)  # with the help of pandas load the data
        logger.debug("Data loaded from %s", data_url) # for printing a message on DEBUg level that data load successfully
        return df       # return with the dataframe where we can save it to a variable

    # if the above try is not done successfully the we handle exception below:
    
    except pd.errors.ParserError as e:
        logger.error("failed to parse the csv file %s", e)
        raise
    except Exception as e:
        logger.error("Unexpected error occurred while loading the data: %s", e)
        raise
    # if there is any error while loading data give me in logging and move forward
    # above method called "Exception Handling"


# Preprocessing method function
def preprocess_data(df: pd.DataFrame)  -> pd.DataFrame:
    """Preprocess the data"""
    try:
        df.drop(columns= ['Unnamed: 2', 'Unnamed: 3', 'Unnamed: 4'], inplace=True)  # dropping unnessery columns
        df.columns = ["target", "text"]     # renaming column names
        logger.debug("Data Prepeocessing Completed")    # printing a message on DEBUG level
        return df
    # above method is not done sucessfully the we will exception below
    except KeyError as e:
        logger.error("Missing column in the DataFrame : %s", e)
        raise
    except Exception as e:
        logger.error("Unexpected error during preprocessing : %s", e)
        raise

# Saving the file
def save_data(train_data: pd.DataFrame, test_data: pd.DataFrame, data_path: str)  -> None: # taking data_path as a parameter
    # "save_data" takes 3 arguments like train_data, test_data and data_path
    """Save the train and test datasets"""   
    try:
        raw_data_path = os.path.join(data_path, "raw")  # using os module we are creating a data_path   
                                                        # creating "raw" folder inside data_path 
        os.makedirs(raw_data_path, exist_ok=True)   # saving DataFrame inside "raw_data_path"
        train_data.to_csv(os.path.join(raw_data_path, "train.csv"), index=False)    # saving "train_data" inside "raw_data_path"
        test_data.to_csv(os.path.join(raw_data_path, "test.csv"), index=False)  # saving "test_data" inside "raw_data_path"
        logger.debug("Train and Test data saved to %s", raw_data_path)
    
    except Exception as e:
        logger.error("Unexpected error occured while saving the data : %s", e)
        raise

def main():
    try:
        test_size = 0.2     # defining test size
        data_path = "https://raw.githubusercontent.com/vikashishere/Datasets/refs/heads/main/spam.csv"  # dataset path definde
        df = load_data(data_url=data_path)  # calling "load_data" function which takes an argument "data_url"
                                            # returns a data in DataFrame where we capture data in "df"
        final_df = preprocess_data(df)  # making final_df by calling "prepeocess_data" giving "df" as argument
                                        # preprocessed data saved to "final_df" variable
        train_data, test_data = train_test_split(final_df, test_size=test_size, random_state=2) # splitting data
        save_data(train_data, test_data, data_path="./data")    # calling "save_data" function
                                            # data_path="./data" means
                                            # . = go to the root of my project and create folder named "data"
    except Exception as e:
        logger.error("Failed to complete the data ingestiom process %s", e)
        print(f"Error : {e}")

if __name__=="__main__":
    main()

# after running all this 2 folders created
# 1. "logs" where our all logs are saves in "data_ingestion.log" file
# 2. "data" folder inside this "raw" subfolder inside this "train_data and test_data" files saved