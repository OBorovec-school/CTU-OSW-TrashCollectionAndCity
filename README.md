# Ontologies and Semantic Web - Trash and Recycling


## Data crawlers 

### Folder structure

(all path are relative to git root)
* config/ - data pipeline configuration files
* data_crawler/ - python source code
* META/ - files for states of pipeline pieces which need that
* log/ - logging output of data pipeline
* results/ - folder with merged rdf files

## Information sources
 Ultimately, it would be the best to cover all czech cities, but for this semester work only the main 3 are covered. You can find more information in the list below. 
 
* Prague:
    * [Map of separated waste](http://opendata.praha.eu/dataset/mapa-trideny-odpad)
        * Content:
            * Location
            * [(Waste type, Container type, Cleaning frequency)]
        * Type: json
    * [Collection points for waste](http://opendata.praha.eu/dataset/praha8-sberna-mista-pro-odpad-a-umisteni-kontejneru/resource/be9bc291-645e-4c8d-a0d1-ba206a05f033)
        * Content:
            * Owner
            * URL
            * Type
            * Opening hours
            * Accepted wastep
            * Contact
            * Location
        * Type: csv
* Brno
    * [Map of collection places and separable waste containers](https://www.sako.cz/sberna-strediska-a-kontejnery/cz/)
        * Content for containers:
            * Location
            * Placement
            * Waste collection frequency and day
            * Count of containers
        * Content for SSO stations:
            * Location
            * Opening hours
            * Contact tel
        * Type: map
        * Notes:
            * There was a problem on Mac with SSL certification - `/Applications/Python\ 3.6/Install\ Certificates.command`
* Plzen
    * [Table of container for separable waste](https://aplikace.plzen.eu/odpady/sep.asp)
        * Content:
            * Location
            * City part
            * Container types
        * Type: HTML table
        * Mergeble with:
            * [Electro waste](https://aplikace.plzen.eu/odpady/ele.asp)
            * [Cloth waste](https://aplikace.plzen.eu/odpady/sat.asp)   
    * [Collection points for waste](https://aplikace.plzen.eu/odpady/sbd.asp)
        * Content:
            * Location
            * City part
    
### How to run it


```python run_data_pipeline.py```
