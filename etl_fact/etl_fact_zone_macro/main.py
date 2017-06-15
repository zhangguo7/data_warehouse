
import configparser


from etl_fact.etl_fact_zone_macro.extract import Extract


def etl_fact_market(**kwargs):

    extract = Extract(kwargs['zone_macro'],kwargs['ht'])
    df_tag_counts = extract.tag_counts()

    for i,sample_tag_counts in df_tag_counts.iterrows():
        grandParentId = sample_tag_counts['sample_tag_counts']
        rent = extract.rent_details(grandParentId)
        industry = extract.industry()



    pass

if __name__ == '__main__':
    db_cfg = configparser.ConfigParser()
    db_cfg.read('../../db.cfg')

    # engine_zone_macro = db_cfg['']
    # engine_draw = db_cfg['']
    # engine_target = db_cfg['']

    etl_fact_market(**db_cfg)