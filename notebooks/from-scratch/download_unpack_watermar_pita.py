from datasets import load_dataset

import os
from concurrent.futures import ThreadPoolExecutor

dataset = load_dataset("bastienp/visible-watermark-pita")

# Function that saves the images to a folder
def save_images(dataset, folder):
    for i, data in enumerate(dataset):
        data["image"].save(f"{folder}/{data['image_id']}.png")

# Function that converts rest of data to pandas dataframe and saves it to a csv file
def save_csv(dataset, filename):
    dataset.select_columns(dataset.column_names[1:]).to_csv(filename, index=False)


# Function that saves the images and the rest of the data to a folder and a csv file
def save_dataset(dataset, dataset_name="data"):
    """
    Create a folder with the name of the dataset
    Save each split of the dataset to a separate folder
    """
    os.makedirs(dataset_name, exist_ok=True)
    with ThreadPoolExecutor() as executor:
        for split, data in dataset.items():
            folder = f"{dataset_name}/{split}"
            os.makedirs(folder, exist_ok=True)
            executor.submit(save_images, data, folder)
            executor.submit(save_csv, data, f"{folder}.csv")

save_dataset(dataset, dataset_name="visible_watermark_pita")