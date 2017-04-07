# mml-muutostietopalvelu-client

Client for NLS's open data change ATOM feed. List available products and loads & keeps datasets up to date

- Install dependencies `go get ./...`
- Build with `go build`

Sample config.toml:
```
api_key = "redacted"
atom_url = "https://tiedostopalvelu.maanmittauslaitos.fi/tp/feed/mtp"
num_workers = 20
```

Sample run:

`./mml-muutostietopalvelu-client/mml-muutostietopalvelu-client  load -p maastotietokanta -t kaikki -f application/gml+xml -d ./mtk`
