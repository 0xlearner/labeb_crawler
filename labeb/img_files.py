import os
import base64
import ast
import json

# directory = "6045_lulu_bahrain_images"


def get_directory():
    for dirpath, dirnames, files in os.walk(os.getcwd()):
        for folder in dirnames:
            if "images" in folder:
                print(folder)
                return folder


def encoded_images(dir, catalog_uuid):
    for d in os.listdir(dir):
        flist = list()
        cat_uuid = d.split("_")[-1]
        if cat_uuid == catalog_uuid:
            print(catalog_uuid)
            path = os.path.join(dir, d)
            for file in os.listdir(os.path.join(path)):
                print(file)
                with open(os.path.join(path, file), "rb") as image_file:
                    encoded_str = base64.b64encode(image_file.read())
                    li_encoded_str = "data:image/jpeg;base64," + encoded_str.decode(
                        "ascii"
                    )
                    flist.append(li_encoded_str)

            return flist


# if "__name__" == "__main__":
directory = get_directory()
catalouge = "135712"
imgs = encoded_images(directory, catalouge)
print(len(imgs))

json_obj = json.dumps({"files": imgs}, sort_keys=True, indent=4, separators=(",", ": "))
with open("encoded.json", "w") as encoded_json:
    encoded_json.write(json_obj)
