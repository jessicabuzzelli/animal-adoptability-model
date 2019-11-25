import pandas as pd
from pprint import pprint
import sqlite3

def getdf(f):
    df = pd.read_csv(f)
    df = df.drop(
        columns='Unnamed: 0,id,animal_type,contact.address.address1,contact.address.address2,contact.address.city,contact.address.country,contact.address.postcode,contact.address.state,contact.email,contact.phone,distance,photos,published_at,status_changed_at,tags,url,organization_id.1,published_at'.split(
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


def getattrs(f):
    df = getdf(f)
    attributes = {}

    for column in df.columns:
        if column not in ['name', 'description']:
            attributes[column] = list(df[column].unique())

    return attributes


def build_db(f):
    conn = sqlite3.connect('animals.db')
    curs = conn.cursor()

    df = getdf(f)
    curs.execute('CREATE TABLE IF NOT EXISTS animal ({})'.format(',\n'.join(list(df.columns) + ['PRIMARY KEY (animal_id, organization_id)'])))

    records = df.values.tolist()

    for row in records:
        try:
            curs.execute('INSERT INTO animal VALUES (?' + ', ?' * 25 + ')', row)
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()
