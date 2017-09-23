# download the Amazon data
wget http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Grocery_and_Gourmet_Food.json.gz -P $HOME
wget http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Grocery_and_Gourmet_Food.json.gz -P $HOME
# unzip the Amazon data
gunzip $HOME/meta_Grocery_and_Gourmet_Food.json.gz
gunzip $HOME/reviews_Grocery_and_Gourmet_Food.json.gz
# copy the Amazon data to DSEFS
dse fs "put $HOME/meta_Grocery_and_Gourmet_Food.json metadata.json"
dse fs "put $HOME/reviews_Grocery_and_Gourmet_Food.json reviews.json"
# remove Amazon data from machine
rm $HOME/metadata.json
rm $HOME/reviews.json
