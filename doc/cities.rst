Documentation: Cities
****************************

The following file contains a basic overview of how parking data is created for each city. For more information regarding data sources, check the "Open Datasets by City" document.


Montréal
========

Service Areas
-------------

The following boroughs of Montréal are covered by Prkng:

 * Ahuntsic-Cartierville
 * Côte-des-Neiges-Notre-Dame-de-Grâce
 * Lachine
 * Mercier-Hochelaga-Maisonneuve
 * Outremont
 * Plateau-Mont-Royal
 * Rivière-des-Prairies-Pointe-aux-Trembles
 * Rosemont-La Petite-Patrie
 * Saint-Laurent
 * Saint-Léonard
 * Le Sud-Ouest
 * Verdun (*)
 * Ville-Marie
 * Villeray-Saint-Michel-Parc-Extension


The following boroughs are **not** covered due to lack of accurate data in the open datasets:

 * Anjou
 * Lasalle
 * Montréal-Nord
 * Pierrefonds-Roxboro
 * L'Île-Bizard-Sainte-Geneviève

The borough of Verdun is a special case as we have a file for blockface restrictions but no individual points in shapefiles that represent signs, so coverage is only partial. Also we do not cover any devolved municipalities or suburbs in Montréal due to lack of data.

In Montréal we cover free and paid on-street parking, residential parking permits in some boroughs (Le Plateau, Mercier-Hochelaga-Maisonneuve, Côte-des-Neiges-Notre-Dame-de-Grâce), as well as a database of off-street parking lots.


Methodology
-----------

The City of Montréal makes available all necessary datasets to easily create a comprehensive parking map. Signs (with individual rules) are keyed to signposts (points on the map) which are used to create the parking lines on the map.

 1. Montréal's road centerline dataset (Géobase) is matched up with divided centerlines from OpenStreetMap by comparing their size, position and names and associating good fits together.
 2. Signs are inserted by pulling rule codes and signpost IDs from the city's provided sign data CSV, joining on the signpost locations and our properly-translated rules table.
 3. Signs for Verdun-Île-des-Soeurs are generated differently: a unique sign is inserted at the midpoint of each blockface, on each side of the street, representing the rule for said blockface in our Verdun CSV.
 4. Signposts are inserted via an aggregation of signs with the same signpost ID.
 5. Signposts are "projected" on-road by joining the signposts dataset with the OSM centerlines.
 6. "Likely" slots are created between all sets of signposts on each blockface, keyed to the signposts they run between.
 7. "Nextpoints" are generated, which calculate the proper signpost to draw slots between for associated rules on each blockface.
 8. Temporary slots are inserted based on the combination of likely slots and nextpoints. These slots include all rule data and proper positioning to the OSM centerlines (plus offset).
 9. Paid rules are associated by finding the nearest temporary slot to a parking meter, unwrapping that slot's rules and inserting the meter's proper paid rules.
 10. Temporary slots are done -- after all cities are processed they will then be aggregated and inserted into the general slots table.


Québec City
===========

Service Areas
-------------

Prkng covers all boroughs of Québec City proper, except the enclave town of L'Ancienne-Lorette and the Wendake reserve. No suburbs are covered due to lack of data.

In Québec City we cover free and paid on-street parking, residential parking permits for the whole city, as well as a database of off-street parking lots.


Methodology
-----------

Québec City makes available all necessary datasets to easily create a comprehensive parking map, with the sole exception of a unique signposts dataset.

 1. Signs are inserted by pulling rule codes and signpost IDs from the city's provided sign data shapefile, joining on our properly-translated rules table.
 2. Signposts are inserted via an aggregation of signs with the same road name, side of street and general location (within 7 meters of each other).
 3. Signposts are "projected" on-road by associating them with the OSM centerlines via their location and similar street name properties.
 4. Paid parking signs and signposts are created by taking the points that represent parking meters, creating small 6 meter slots in front of each one, and inserting the endpoints of such slots as general signposts.
 5. "Likely" slots are created between all sets of signposts on each blockface, keyed to the signposts they run between.
 6. "Nextpoints" are generated, which calculate the proper signpost to draw slots between for associated rules on each blockface.
 7. Temporary slots are inserted based on the combination of likely slots and nextpoints. These slots include all rule data and proper positioning to the OSM centerlines (plus offset).
 8. Temporary slots are done -- after all cities are processed they will then be aggregated and inserted into the general slots table.


New York City
=============

Service Areas
-------------

Prkng covers all boroughs of New York City. No suburbs are covered due to lack of data.

In New York City we cover free and paid on-street parking, commercial parking permits for the whole city (there is no residential permit program in New York), as well as a database of off-street parking lots through our partnership with Parking Panda.


Methodology
-----------

The City of New York makes available all necessary datasets to easily create a comprehensive parking map, with the sole exception of a unique signposts dataset.

 1. New York's road centerline dataset is matched up with divided centerlines from OpenStreetMap by comparing their size, position and names and associating good fits together.
 2. Signs are inserted by pulling rule codes and signpost IDs from the city's provided sign data shapefile, joining on our properly-translated rules table. Sign directions are calculated by matching the cardinal direction of the street centerline with the cardinal arrow direction on the sign dataset.
 3. Signposts are inserted via an aggregation of signs with the same blockface ID and distance from the start of the blockface.
 4. Signposts are "projected" on-road by joining the signposts dataset with the OSM centerlines.
 5. "Likely" slots are created between all sets of signposts on each blockface, keyed to the signposts they run between.
 6. "Nextpoints" are generated, which calculate the proper signpost to draw slots between for associated rules on each blockface.
 7. Temporary slots are inserted based on the combination of likely slots and nextpoints. These slots include all rule data and proper positioning to the OSM centerlines (plus offset).
 8. Temporary slots are done -- after all cities are processed they will then be aggregated and inserted into the general slots table.


Seattle
=======

Service Areas
-------------

Prkng covers all neighbourhoods of Seattle. Areas of best coverage are: Downtown, Belltown, Capitol Hill, South Lake Union, Lower Queen Anne, the Central District, the International District, and central parts of Ballard, Fremont, the University District and West Seattle. Other neighbourhoods are covered with blockface restrictions so some context may be lost, especially for drop-off zones. No suburbs are covered due to lack of data.

In Seattle we cover free and paid on-street parking, residential parking permits for most of the city, as well as a database of off-street parking lots that have occupancy numbers updated in real time via the City of Seattle.


Methodology
-----------

The City of Seattle makes available several interesting datasets that we use for the creation of a comprehensive parking map, including signpoints, curb lines, general blockface restrictions, and more. The signpoints dataset is full of errors and imprecise so we cannot rely on it alone to create a good set of data.

 1. Seattle's road centerline dataset is matched up with divided centerlines from OpenStreetMap by comparing their size, position and names and associating good fits together.
 2. The first set of signs is inserted by associating curb lines to the nearest appropriate sign in the signpoints table, then creating signs at either end of said curb line using our rules table. This is then done a second time for paid parking, instead of associating the curb lines with signpoints they are associated with general blockface restrictions, which have paid parking rates and hours. These rates and hours are processed into dynamically-generated rules before processing.
 3. The second set of signs are created by gathering signpoints which have specific directional requirements ("NO PARKING NORTH OF HERE") and inserting them as-is with the proper direction.
 4. The third set of signs are created by using the general blockface restrictions dataset, on streets that do not have curb line data. This includes no parking, time limited and residential parking zone restrictions. Signs are inserted at the midpoint each blockface in both directions, to cover up until the end of the block OR the next directional sign as created in step 3. The proper rule is gathered by pairing the blockface restriction type with the most appropriate sign on the same blockface. This assumes that these types of rules will be the same for the entire street. For 'unrestricted' blockfaces, a bidirectional "PARKING AUTHORIZED AT ALL TIMES" sign is inserted so the slot will still show up on the map.
 5. Directions are assigned to all signs if they do not already have one, by calculating the cardinal direction of the street centerline and giving the appropriate value compared with the cardinal direction the sign might have.
 6. Signposts are inserted via an aggregation of signs with the same blockface and distance from the start of the street.
 7. Signposts are "projected" on-road by joining the signposts dataset with the OSM centerlines.
 8. "Likely" slots are created between all sets of signposts on each blockface, keyed to the signposts they run between.
 9. "Nextpoints" are generated, which calculate the proper signpost to draw slots between for associated rules on each blockface.
 10. Temporary slots are inserted based on the combination of likely slots and nextpoints. These slots include all rule data and proper positioning to the OSM centerlines (plus offset).
 11. Temporary slots are done -- after all cities are processed they will then be aggregated and inserted into the general slots table.


Boston
======

Service Areas
-------------

Prkng covers all neighbourhoods of Boston city proper, as well as the city of Cambridge. The cities of Brookline and Somerville have enough data but cannot yet be included due to a lack of address point data (to determine sides of the street). No other suburbs are covered due to lack of data.

In Boston we cover street-sweeping rules for on-street parking, paid on-street parking, as well as a database of off-street parking lots via our own data collection and our partnership with Parking Panda.


Methodology
-----------

1. Boston's road centerline dataset is matched up with divided centerlines from OpenStreetMap by comparing their size, position and names and associating good fits together.
2. Signs are inserted after a multi-step process, as rules apply to multiple sets of blockfaces between certain streets. First, centerlines are grouped together into MultiLineStrings by their street ID, then they are cut at the intersections of the appropriate streets. A buffer is created along the remaining line, and any centerlines found within this buffer are now associated with the rule. Individual signs are inserted on the midpoint of each side of the street, pointing in both directions. Rules are paired by joining with our rule dataset, which has the ID of the rule in its rule code.
3. For Cambridge, signs are inserted by pairing the centerline with the street sweeping district buffers they find themselves inside. Individual signs are inserted on the midpoint of each side of the street, pointing in both directions. Rules are paired by joining with our rule dataset, which has the sweeping district ID and side of street in its rule code.
4. Signposts are inserted via an aggregation of signs with the same blockface ID and distance from the start of the blockface.
5. Signposts are "projected" on-road by joining the signposts dataset with the OSM centerlines.
6. "Likely" slots are created between all sets of signposts on each blockface, keyed to the signposts they run between.
7. "Nextpoints" are generated, which calculate the proper signpost to draw slots between for associated rules on each blockface.
8. Temporary slots are inserted based on the combination of likely slots and nextpoints. These slots include all rule data and proper positioning to the OSM centerlines (plus offset).
9. Paid rules are associated by finding the nearest temporary slot to a parking meter, unwrapping that slot's rules and inserting the meter's proper paid rules.
10. Temporary slots are done -- after all cities are processed they will then be aggregated and inserted into the general slots table.
