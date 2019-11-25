from API_KEYS import CLIENT_ID, SECRET
import requests
from pandas.io.json import json_normalize

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

    def getanimals(self, animal_type=None, pages=100, results_per_page=100):
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
                # TODO - figure out why not looping
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

            df = self.export_df({'animals': animals})
            return df

        except KeyError:
            print('Empty response.')
            return None

    def export_df(self, results):
        results_df = json_normalize(results['animals'])

        results_df['_links.organization.href'] = results_df['_links.organization.href'].str.replace('/v2/organizations/', '')
        results_df['_links.self.href'] = results_df['_links.self.href'].str.replace('/v2/animals/', '')
        results_df['_links.type.href'] = results_df['_links.type.href'].str.replace('/v2/types/', '')

        results_df.rename(columns={'_links.organization.href': 'organization_id',
                                   '_links.self.href': 'animal_id',
                                   '_links.type.href': 'animal_type'}, inplace=True)

        return results_df

def export_csv(outf):
    pf = ScrapePetFinder(CLIENT_ID, SECRET)

    try:
        df = pf.getanimals(animal_type='dog')
        df.to_csv(outf)

    except AttributeError:
        pass

# export_csv('animaldata1.csv')
