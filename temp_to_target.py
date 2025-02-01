from target.country import *
from target.region import *
from target.customer import *
from target.category import *
from target.product import *
from target.subcategory import *
from target.sales import *
from target.location import *
def main():
    load_country_to_tgt()
    load_region_to_tgt()
    load_customer_to_tgt()
    load_category_to_tgt()
    load_subcategory_to_tgt()
    load_product_to_tgt()
    load_location_to_tgt()
    load_sales_to_tgt()
if __name__ == '__main__':
    main()