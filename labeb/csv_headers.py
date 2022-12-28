import csv

with open("final_labeb_items.csv", "r") as inputfile, open(
    "output.csv", "w", newline=""
) as outputfile:
    csv_in = csv.reader(inputfile)
    csv_out = csv.writer(outputfile)
    title = next(csv_in)
    csv_out.writerow(title)
    for row in csv_in:
        if row != title:
            csv_out.writerow(row)
