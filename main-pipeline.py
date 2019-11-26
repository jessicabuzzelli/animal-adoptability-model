from API_KEYS import CLIENT_ID, SECRET
import requests
from pandas.io.json import json_normalize
import pandas as pd
import sqlite3


class ScrapePetFinder:

    def __init__(self, CLIENT_ID, SECRET):
        self.url = 'http://api.petfinder.com/v2/'
        self.auth = self.getauth(CLIENT_ID, SECRET)

    def getauth(self, CLIENT_ID, SECRET):
        data = {'grant_type': 'client_credentials',
                'client_id': CLIENT_ID,
                'client_secret': SECRET}

        r = requests.post(self.url + 'oauth2/token', data=data).json()
        return r['access_token']

    def getanimals(self, outf, animal_type=None, pages=100, results_per_page=100):
        params = {'animal_type': animal_type,
                  'pages': 1,
                  'results_per_page': results_per_page}
        headers = {'Authorization': 'Bearer {}'.format(self.auth)}

        r = requests.get(url=self.url + 'animals/',
                         headers=headers,
                         params=params)
        # print(r.json())

        try:
            animals = r.json()['animals']
            num_pages = r.json()['pagination']['total_pages']
            print('Number of pages: {}'.format(str(num_pages)))

            for page in range(num_pages - 2, 2, -1):  # get oldest data first -- more adopted data?
                # TODO - fix pagination loop
                print(page)
                params['page'] = page
                r = requests.get(self.url + 'animals/',
                                headers=headers,
                                params=params)

                try:
                    for record in r.json()['animals']:
                        animals.append(record)

                except KeyError:
                    break

            df = self.export_df({'animals': animals}, outf)
            return df

        except KeyError:
            print('Empty response.')
            return None

    def export_df(self, results, outf):
        df = json_normalize(results['animals'])

        df['_links.organization.href'] = df['_links.organization.href'].str.replace('/v2/organizations/', '')
        df['_links.self.href'] = df['_links.self.href'].str.replace('/v2/animals/', '')
        df['_links.type.href'] = df['_links.type.href'].str.replace('/v2/types/', '')

        df.rename(columns={'_links.organization.href': 'organization_id',
                                   '_links.self.href': 'animal_id',
                                   '_links.type.href': 'animal_type'}, inplace=True)
        # df.to_csv(outf)
        return df


class MainPipeline:

    def __init__(self, f=None):
        self.conn = sqlite3.connect('animals.db')
        self.curs = self.conn.cursor()

        if f is None:
            pf = ScrapePetFinder(CLIENT_ID, SECRET)
            df = pf.getanimals(outf='dogs_again.csv', animal_type='dog')

        else:
            df = pd.read_csv(f)

        df = self.reformat_df(df)

        self.build_db(df, table='Dog')
        self.build_db(df, table='Cat')

        self.binary_labels('dog')
        self.binary_labels('cat')

        self.conn.close()

    def reformat_df(self, df):
        df = df.drop(
            columns='type,Unnamed: 0,id,animal_type,contact.address.address1,contact.address.address2,contact.address.city,contact.address.country,contact.address.postcode,contact.address.state,contact.email,contact.phone,distance,photos,published_at,status_changed_at,tags,url,organization_id.1,published_at'.split(
                ','))
        df = df.fillna('False').replace(to_replace='False', value=False).rename(
            columns={"attributes.declawed": "declawed", "attributes.house_trained": "house_trained",
                     'attributes.shots_current': 'shots_current',
                     'attributes.spayed_neutered': 'spayed_neutered',
                     'attributes.special_needs': 'special_needs', 'breeds.mixed': 'mixed_breed',
                     'breeds.primary': 'primary_breed', 'breeds.secondary': 'secondary_breed',
                     'breeds.unknown': 'unknown_breed', 'colors.primary': 'primary_color',
                     'colors.secondary': 'secondary_color', 'colors.tertiary': 'tertiary_color',
                     'environment.cats': 'friendly_to_cats', 'environment.dogs': 'friendly_to_dogs',
                     'environment.children': 'friendly_to_children'})

        return df

    def build_db(self, df, table):
        sql = 'CREATE TABLE IF NOT EXISTS ' + table.lower() + ' ({})' \
            .format(',\n'.join(list(df.columns) + ['PRIMARY KEY (animal_id, organization_id)']))

        self.curs.execute(sql)

        records = df[df['species'] == table].values.tolist()

        for row in records:
            try:  # loop is fast but see if curs.executemany() can handle exceptions?
                sql = 'INSERT INTO ' + table + ' VALUES (?' + ', ?' * 24 + ')'
                self.curs.execute(sql, row)

            except sqlite3.IntegrityError:
                pass

        self.conn.commit()

    def binary_labels(self, table):
        columns = pd.read_sql('PRAGMA table_info({})'.format(table), self.conn)['name'].tolist()
        attrs = {}

        for column in columns:
            if column not in ['name', 'description', 'organization_id', 'animal_id']:
                attrs[column] = self.curs.execute('select distinct {}  from {}'.format(column, table)).fetchall()

        tf = []
        continuous_labels = []

        for key in attrs.keys():
            if [(0,), (1,)] == attrs[key] or [(1,), (0,)] == attrs[key]:
                tf.append(key)

            else:
                continuous_labels.append(key)

        for key in continuous_labels:
            for attr in attrs[key]:
                attr = self.sanitize_attrs(attr)

                for a in attr:
                    if a not in ['', 'and', 'dog', 'cat'] and a not in columns:
                        sql = ['''alter table {} add column {}'''.format(table, a),
                               '''update {} set {} = 0'''.format(table, a),
                               '''update {} set {} = 1 where {} = {}'''.format(table, a, key, a),  # TODO - get to work
                               ]

                        for s in sql:
                            try:
                                self.curs.execute(s)
                                columns.append(a)
                            except sqlite3.OperationalError:
                                print(s)

        self.conn.commit()

    def sanitize_attrs(self, attr):
        attr = str(attr[0]).strip('0').strip('-').split(' ')  # returns list

        if len(attr) > 1:
            attr = [word for word in attr if word.isalnum() is True]  # only add if no symbols

        return attr


pl = MainPipeline('dogs.csv')
