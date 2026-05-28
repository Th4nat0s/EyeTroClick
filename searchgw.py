
class TelegramSearch(BaseView):
    @expose('/fetch_telegrams', methods=['POST'])
    def api_process_claimed_victim(self):
        '''
        Route API pour sortir tous les message collectes mis en forme pour analyse ( eyetelex output )
        demande au backend, attention si message empty ou > à 2 ans, il est discard (voir eyetroclick pour la génération)
        
        parametre since = timestmap
        for = duration in minute ( "since" < "since + for" )

        # curl -X POST -H "Content-Type: application/json" -d '{"api_key": "zoubida", "since": "1749342874" , "for": "15"}' http://127.0.0.1:5000/telegramsearch/fetch_telegrams
        '''
        # Récupérez le JSON
        data = request.get_json()
        auth_valid = None
        key = ""
        # check minima
        if 'api_key' in data and 'since' in data and 'for' in data:
            key = data['api_key']
            # check Api KEy
            auth_valid = db.session.query(ApiKeys).filter(ApiKeys.key == key, ApiKeys.active == True).first()
        if auth_valid:
            since = data['since']
            tfor = data['for']

            try:
                int(since)
                int(tfor)
            except ValueError:
                return jsonify({'Error': 'Bad parameters'}), 400

            # Max 2h
            if int(tfor) > 20*60:
                tfor = "1200"

            url = f"http://{db.app.config.get('CH_HOST')}:{db.app.config.get('CH_PORT')}/last?since={since}&for={tfor}"

            # Return l'object recu au fur et a mesure, testé avec le l’output a million.
            def generate():
                try:
                    with requests.get(url, stream=True) as response:
                        for chunk in response.iter_content(chunk_size=8192):
                            yield chunk
                except requests.exceptions.ConnectionError:
                    yield b'{"Error": "Connection issues to the backend"}'

            return Response(generate(), content_type='application/json')

        else:
            return jsonify({'Error': 'Not Authorized or bad parameters'}), 401


    @expose('/search_telegrams/')
    @has_access
    def list(self):
        url = f"http://{db.app.config.get('CH_HOST')}:{db.app.config.get('CH_PORT')}/count"
        try:
            response = requests.get(url)
            data = response.json()
        except requests.exceptions.ConnectionError:
            return render_template('error.html', base_template=appbuilder.base_template, appbuilder=appbuilder)
        return render_template('list_search.html', base_template=appbuilder.base_template, appbuilder=appbuilder, data = data)


    @expose('/search_go_telegrams', methods=['POST'])
    @has_access  # toute personne authentifiée
    def search_go_telegrams(self):

        # Récupérez le JSON
        data = request.get_json()
        field = data.get('field')
        value = urllib.parse.quote_plus(data.get('value'))
        method = data.get('method')  # IS or LIKE
        count = int(data.get('count')) + 1
        count = str(count)
        # , default=100, type=int)  # Max responses, default is 100

        # Logique de recherche ici, par exemple :
        # results = perform_search(field, value, method, count)
        # Pour cet exemple, nous allons juste retourner les paramètres reçus

        url = f"http://{db.app.config.get('CH_HOST')}:{db.app.config.get('CH_PORT')}/search"
        query = f"?field={field}&value={value}&method={method}&count={count}"

        try:
            response = requests.get(url+query)
            data = response.json()
        except requests.exceptions.ConnectionError:
            return render_template('error.html', base_template=appbuilder.base_template, appbuilder=appbuilder)
        return jsonify(data)
