"""
Metro Area Configuration for Marketing Lead Finder

Contains city/neighborhood lists, ZIP prefixes, and search locations
for all supported US metro areas.
"""

from dataclasses import dataclass
from typing import Dict, List


@dataclass
class MetroConfig:
    """Configuration for a single metro area"""
    key: str                    # Cache key identifier (e.g., "los_angeles")
    display_name: str           # UI display name (e.g., "Greater Los Angeles Area")
    area_cities: List[str]      # Cities/neighborhoods for location validation (lowercase)
    zip_prefixes: List[str]     # ZIP code prefixes for validation
    search_locations: List[str] # Indeed search locations
    state_abbrev: str           # Primary state abbreviation (e.g., "CA")
    non_metro_cities: List[str] # Cities to exclude (same-state cities in other metros)


# =============================================================================
# METRO CONFIGURATIONS
# =============================================================================

METRO_CONFIGS: Dict[str, MetroConfig] = {

    # -------------------------------------------------------------------------
    # ATLANTA
    # -------------------------------------------------------------------------
    "atlanta": MetroConfig(
        key="atlanta",
        display_name="Greater Atlanta Area",
        area_cities=[
            # Core Atlanta
            "atlanta", "midtown", "buckhead", "downtown atlanta",
            # Major suburbs
            "marietta", "decatur", "sandy springs", "roswell", "alpharetta",
            "johns creek", "dunwoody", "brookhaven", "smyrna", "kennesaw",
            "lawrenceville", "duluth", "suwanee", "peachtree city", "newnan",
            "douglasville", "woodstock", "canton", "acworth", "austell",
            "lithia springs", "mableton", "powder springs", "vinings",
            "stone mountain", "tucker", "chamblee", "doraville", "clarkston",
            "avondale estates", "college park", "east point", "hapeville",
            "forest park", "riverdale", "jonesboro", "stockbridge", "mcdonough",
            "conyers", "covington", "snellville", "lilburn", "norcross",
            "peachtree corners", "buford", "flowery branch", "gainesville",
        ],
        zip_prefixes=["300", "301", "302", "303", "304", "305", "306", "311", "312"],
        search_locations=["Atlanta, GA", "Marietta, GA", "Decatur, GA", "Sandy Springs, GA", "Alpharetta, GA"],
        state_abbrev="GA",
        non_metro_cities=["savannah", "augusta", "macon", "columbus", "athens"],
    ),

    # -------------------------------------------------------------------------
    # AUSTIN
    # -------------------------------------------------------------------------
    "austin": MetroConfig(
        key="austin",
        display_name="Greater Austin Area",
        area_cities=[
            # Core Austin
            "austin", "downtown austin", "south austin", "east austin", "north austin",
            # Major suburbs
            "round rock", "cedar park", "georgetown", "pflugerville", "leander",
            "san marcos", "kyle", "buda", "dripping springs", "lakeway",
            "bee cave", "west lake hills", "rollingwood", "sunset valley",
            "manor", "elgin", "bastrop", "taylor", "hutto",
            "liberty hill", "jarrell", "florence", "granger",
            # Neighborhoods
            "mueller", "domain", "arboretum", "barton hills", "zilker",
            "hyde park", "tarrytown", "clarksville", "south congress", "east riverside",
        ],
        zip_prefixes=["786", "787", "788", "789", "765"],
        search_locations=["Austin, TX", "Round Rock, TX", "Cedar Park, TX", "Georgetown, TX", "San Marcos, TX"],
        state_abbrev="TX",
        non_metro_cities=["dallas", "houston", "san antonio", "fort worth", "el paso"],
    ),

    # -------------------------------------------------------------------------
    # BAY AREA (San Francisco / Oakland / San Jose)
    # -------------------------------------------------------------------------
    "bay_area": MetroConfig(
        key="bay_area",
        display_name="Greater Bay Area",
        area_cities=[
            # San Francisco
            "san francisco", "soma", "mission district", "castro", "marina",
            "pacific heights", "financial district", "north beach", "chinatown",
            # East Bay
            "oakland", "berkeley", "fremont", "hayward", "richmond",
            "san leandro", "concord", "walnut creek", "pleasanton", "livermore",
            "alameda", "emeryville", "union city", "newark", "dublin",
            "san ramon", "danville", "moraga", "orinda", "lafayette",
            "antioch", "pittsburg", "brentwood", "el cerrito", "albany",
            # South Bay / Silicon Valley
            "san jose", "palo alto", "mountain view", "sunnyvale", "santa clara",
            "cupertino", "milpitas", "campbell", "los gatos", "saratoga",
            "los altos", "menlo park", "redwood city", "san mateo", "foster city",
            "burlingame", "san carlos", "belmont", "half moon bay", "daly city",
            "south san francisco", "san bruno", "pacifica",
            # North Bay
            "san rafael", "novato", "petaluma", "santa rosa", "napa",
            "vallejo", "fairfield", "vacaville",
        ],
        zip_prefixes=[
            "940", "941", "942", "943", "944", "945", "946", "947", "948", "949",
            "950", "951", "952", "953", "954", "955", "956", "957", "958", "959",
            "945", "946", "947", "948", "949",
        ],
        search_locations=["San Francisco, CA", "Oakland, CA", "San Jose, CA", "Palo Alto, CA", "Fremont, CA"],
        state_abbrev="CA",
        non_metro_cities=["los angeles", "san diego", "sacramento", "fresno", "bakersfield"],
    ),

    # -------------------------------------------------------------------------
    # BOSTON
    # -------------------------------------------------------------------------
    "boston": MetroConfig(
        key="boston",
        display_name="Greater Boston Area",
        area_cities=[
            # Core Boston
            "boston", "downtown boston", "back bay", "beacon hill", "south end",
            "north end", "seaport", "financial district", "fenway", "kenmore",
            "allston", "brighton", "jamaica plain", "roxbury", "dorchester",
            "south boston", "charlestown", "east boston", "mattapan", "roslindale",
            "west roxbury", "hyde park",
            # Cambridge / Somerville
            "cambridge", "somerville", "medford", "malden", "everett",
            "chelsea", "revere", "winthrop",
            # Inner suburbs
            "brookline", "newton", "watertown", "waltham", "belmont",
            "arlington", "lexington", "winchester", "woburn", "burlington",
            "reading", "stoneham", "melrose", "wakefield", "saugus", "lynn",
            # Outer suburbs
            "quincy", "braintree", "weymouth", "milton", "dedham", "needham",
            "wellesley", "natick", "framingham", "marlborough", "weston",
            "wayland", "sudbury", "concord", "acton", "bedford", "billerica",
            "lowell", "lawrence", "haverhill", "andover", "north andover",
            "peabody", "salem", "beverly", "gloucester", "marblehead",
        ],
        zip_prefixes=["010", "011", "012", "013", "014", "015", "016", "017", "018", "019",
                      "020", "021", "022", "023", "024", "025", "026", "027"],
        search_locations=["Boston, MA", "Cambridge, MA", "Quincy, MA", "Brookline, MA", "Newton, MA"],
        state_abbrev="MA",
        non_metro_cities=["springfield", "worcester", "providence", "hartford"],
    ),

    # -------------------------------------------------------------------------
    # CHICAGO
    # -------------------------------------------------------------------------
    "chicago": MetroConfig(
        key="chicago",
        display_name="Greater Chicago Area",
        area_cities=[
            # Core Chicago
            "chicago", "loop", "river north", "streeterville", "gold coast",
            "lincoln park", "lakeview", "wicker park", "bucktown", "logan square",
            "west loop", "south loop", "pilsen", "bridgeport", "bronzeville",
            "hyde park", "rogers park", "edgewater", "uptown", "ravenswood",
            # North suburbs
            "evanston", "skokie", "wilmette", "winnetka", "glencoe",
            "highland park", "lake forest", "deerfield", "northbrook", "glenview",
            "park ridge", "des plaines", "niles", "morton grove", "lincolnwood",
            # Northwest suburbs
            "schaumburg", "arlington heights", "palatine", "hoffman estates",
            "rolling meadows", "elk grove village", "mount prospect", "buffalo grove",
            # West suburbs
            "oak park", "oak brook", "naperville", "aurora", "wheaton",
            "glen ellyn", "lombard", "downers grove", "lisle", "woodridge",
            "bolingbrook", "elmhurst", "la grange", "hinsdale", "western springs",
            "berwyn", "cicero", "forest park", "riverside", "brookfield",
            # South suburbs
            "oak lawn", "orland park", "tinley park", "frankfort", "mokena",
            "new lenox", "joliet", "homer glen", "palos hills", "evergreen park",
        ],
        zip_prefixes=["600", "601", "602", "603", "604", "605", "606", "607", "608", "609"],
        search_locations=["Chicago, IL", "Naperville, IL", "Evanston, IL", "Oak Brook, IL", "Schaumburg, IL"],
        state_abbrev="IL",
        non_metro_cities=["springfield", "peoria", "rockford", "champaign", "bloomington"],
    ),

    # -------------------------------------------------------------------------
    # DALLAS-FORT WORTH
    # -------------------------------------------------------------------------
    "dallas": MetroConfig(
        key="dallas",
        display_name="Greater Dallas Area",
        area_cities=[
            # Core Dallas
            "dallas", "downtown dallas", "uptown dallas", "deep ellum",
            "oak lawn", "highland park", "university park", "preston hollow",
            "lakewood", "oak cliff", "bishop arts",
            # Fort Worth
            "fort worth", "downtown fort worth", "sundance square",
            # Major suburbs
            "plano", "irving", "arlington", "garland", "frisco", "mckinney",
            "richardson", "carrollton", "lewisville", "denton", "allen",
            "flower mound", "coppell", "grapevine", "southlake", "keller",
            "colleyville", "bedford", "euless", "hurst", "north richland hills",
            "grand prairie", "mesquite", "rowlett", "rockwall", "wylie",
            "sachse", "murphy", "prosper", "celina", "little elm",
            "the colony", "addison", "farmers branch", "duncanville",
            "desoto", "cedar hill", "lancaster", "mansfield", "burleson",
            "weatherford", "cleburne", "midlothian", "waxahachie",
        ],
        zip_prefixes=["750", "751", "752", "753", "754", "755", "756", "757", "758", "759",
                      "760", "761", "762", "763", "764", "765", "766", "767", "768", "769",
                      "750", "751", "752", "760", "761", "762", "763", "764", "765", "766", "767", "768"],
        search_locations=["Dallas, TX", "Fort Worth, TX", "Plano, TX", "Irving, TX", "Arlington, TX"],
        state_abbrev="TX",
        non_metro_cities=["houston", "austin", "san antonio", "el paso"],
    ),

    # -------------------------------------------------------------------------
    # HOUSTON
    # -------------------------------------------------------------------------
    "houston": MetroConfig(
        key="houston",
        display_name="Greater Houston Area",
        area_cities=[
            # Core Houston
            "houston", "downtown houston", "midtown", "montrose", "heights",
            "river oaks", "galleria", "uptown", "memorial", "tanglewood",
            "west university place", "bellaire", "rice village", "museum district",
            "east downtown", "eado", "third ward", "second ward", "fifth ward",
            # Major suburbs
            "sugar land", "the woodlands", "katy", "pearland", "league city",
            "pasadena", "baytown", "missouri city", "friendswood", "cypress",
            "spring", "humble", "kingwood", "atascocita", "tomball",
            "conroe", "clear lake", "webster", "seabrook", "kemah",
            "richmond", "rosenberg", "stafford", "alvin", "angleton",
            "galveston", "texas city", "la marque", "dickinson", "santa fe",
            "deer park", "la porte", "channelview", "jacinto city",
        ],
        zip_prefixes=["770", "771", "772", "773", "774", "775", "776", "777", "778", "779",
                      "773", "774", "775"],
        search_locations=["Houston, TX", "Sugar Land, TX", "The Woodlands, TX", "Katy, TX", "Pearland, TX"],
        state_abbrev="TX",
        non_metro_cities=["dallas", "austin", "san antonio", "fort worth", "el paso"],
    ),

    # -------------------------------------------------------------------------
    # LOS ANGELES
    # -------------------------------------------------------------------------
    "los_angeles": MetroConfig(
        key="los_angeles",
        display_name="Greater Los Angeles Area",
        area_cities=[
            # Core LA
            "los angeles",
            # LA neighborhoods and cities
            "hollywood", "west hollywood", "beverly hills",
            "santa monica", "venice", "culver city", "marina del rey",
            "pasadena", "glendale", "burbank", "north hollywood", "studio city",
            "sherman oaks", "encino", "van nuys", "woodland hills", "calabasas",
            "malibu", "brentwood", "westwood", "century city", "koreatown",
            "silver lake", "echo park", "los feliz", "eagle rock", "highland park",
            "atwater village", "boyle heights", "watts",
            "compton", "inglewood", "hawthorne", "el segundo", "manhattan beach",
            "hermosa beach", "redondo beach", "torrance", "carson", "long beach",
            "lakewood", "downey", "whittier", "cerritos", "norwalk",
            "la mirada", "fullerton", "anaheim", "irvine", "costa mesa",
            "newport beach", "huntington beach", "orange", "tustin", "santa ana",
            # Valleys and outlying areas
            "pomona", "covina", "west covina", "diamond bar", "claremont",
            "azusa", "monrovia", "arcadia", "alhambra", "monterey park",
            "el monte", "baldwin park", "san dimas", "glendora", "duarte",
            "rancho cucamonga", "ontario", "upland", "fontana",
            # Ventura County (close to LA)
            "ventura", "oxnard", "thousand oaks", "simi valley", "camarillo",
        ],
        zip_prefixes=["900", "901", "902", "903", "904", "905", "906", "907", "908", "909",
                      "910", "911", "912", "913", "914", "915", "916", "917", "918", "919",
                      "920", "921", "922", "923", "924", "925", "926", "927", "928"],
        search_locations=["Los Angeles, CA", "Santa Monica, CA", "Burbank, CA", "Pasadena, CA", "Irvine, CA"],
        state_abbrev="CA",
        non_metro_cities=["san francisco", "san jose", "oakland", "sacramento",
                          "san diego", "fresno", "bakersfield", "palo alto",
                          "mountain view", "sunnyvale", "cupertino", "berkeley",
                          "walnut creek", "concord", "fremont", "hayward"],
    ),

    # -------------------------------------------------------------------------
    # MIAMI
    # -------------------------------------------------------------------------
    "miami": MetroConfig(
        key="miami",
        display_name="Greater Miami Area",
        area_cities=[
            # Core Miami
            "miami", "downtown miami", "brickell", "wynwood", "midtown miami",
            "little havana", "coral gables", "coconut grove", "key biscayne",
            "south beach", "miami beach", "north miami", "north miami beach",
            # Broward County
            "fort lauderdale", "hollywood", "pompano beach", "deerfield beach",
            "boca raton", "delray beach", "boynton beach", "coral springs",
            "plantation", "sunrise", "davie", "pembroke pines", "miramar",
            "weston", "cooper city", "dania beach", "hallandale beach",
            "lauderhill", "tamarac", "margate", "coconut creek", "parkland",
            # Palm Beach
            "west palm beach", "palm beach gardens", "jupiter", "wellington",
            "royal palm beach", "lake worth", "greenacres", "riviera beach",
            # Miami-Dade suburbs
            "hialeah", "miami gardens", "homestead", "kendall", "doral",
            "aventura", "sunny isles beach", "bal harbour", "surfside",
            "pinecrest", "palmetto bay", "cutler bay", "miami lakes",
            "sweetwater", "fontainebleau", "westchester", "tamiami",
        ],
        zip_prefixes=["330", "331", "332", "333", "334", "335", "336", "337", "338", "339",
                      "334", "335"],
        search_locations=["Miami, FL", "Fort Lauderdale, FL", "Boca Raton, FL", "West Palm Beach, FL", "Coral Gables, FL"],
        state_abbrev="FL",
        non_metro_cities=["tampa", "orlando", "jacksonville", "tallahassee", "naples"],
    ),

    # -------------------------------------------------------------------------
    # NEW YORK
    # -------------------------------------------------------------------------
    "new_york": MetroConfig(
        key="new_york",
        display_name="Greater New York Area",
        area_cities=[
            # Manhattan
            "manhattan", "new york", "new york city", "nyc", "midtown",
            "times square", "chelsea", "soho", "tribeca", "financial district",
            "wall street", "greenwich village", "east village", "lower east side",
            "upper east side", "upper west side", "harlem", "washington heights",
            "inwood", "flatiron", "gramercy", "murray hill", "hell's kitchen",
            # Brooklyn
            "brooklyn", "williamsburg", "dumbo", "brooklyn heights", "park slope",
            "bushwick", "bed-stuy", "crown heights", "flatbush", "greenpoint",
            "cobble hill", "carroll gardens", "red hook", "sunset park",
            "bay ridge", "bensonhurst", "coney island", "brighton beach",
            # Queens
            "queens", "astoria", "long island city", "flushing", "jamaica",
            "forest hills", "rego park", "jackson heights", "elmhurst",
            "corona", "woodside", "sunnyside", "bayside", "fresh meadows",
            # Bronx
            "bronx", "riverdale", "fordham", "pelham bay", "throggs neck",
            # Staten Island
            "staten island", "st. george", "tottenville",
            # Long Island
            "long island", "garden city", "hempstead", "great neck", "manhasset",
            "oyster bay", "huntington", "smithtown", "islip", "babylon",
            # Westchester
            "white plains", "yonkers", "new rochelle", "scarsdale", "tarrytown",
            "dobbs ferry", "bronxville", "mount vernon", "rye", "harrison",
            # New Jersey (close to NYC)
            "jersey city", "hoboken", "newark", "weehawken", "fort lee",
            "edgewater", "englewood", "teaneck", "hackensack", "paramus",
            "ridgewood", "montclair", "west orange", "livingston", "morristown",
            "princeton", "new brunswick", "edison", "woodbridge",
        ],
        zip_prefixes=["100", "101", "102", "103", "104", "105", "106", "107", "108", "109",
                      "110", "111", "112", "113", "114", "115", "116", "117", "118", "119",
                      "070", "071", "072", "073", "074", "075", "076", "077", "078", "079",
                      "088", "089"],
        search_locations=["New York, NY", "Brooklyn, NY", "Manhattan, NY", "Jersey City, NJ", "White Plains, NY"],
        state_abbrev="NY",
        non_metro_cities=["buffalo", "albany", "rochester", "syracuse", "philadelphia", "boston"],
    ),

    # -------------------------------------------------------------------------
    # PHILADELPHIA
    # -------------------------------------------------------------------------
    "philadelphia": MetroConfig(
        key="philadelphia",
        display_name="Greater Philadelphia Area",
        area_cities=[
            # Core Philadelphia
            "philadelphia", "center city", "old city", "rittenhouse",
            "university city", "fishtown", "northern liberties", "south philly",
            "manayunk", "chestnut hill", "roxborough", "germantown", "mt. airy",
            "west philadelphia", "kensington", "port richmond", "bridesburg",
            # Pennsylvania suburbs
            "king of prussia", "conshohocken", "norristown", "ardmore",
            "bryn mawr", "wayne", "malvern", "west chester", "media",
            "swarthmore", "springfield", "chester", "upper darby", "drexel hill",
            "havertown", "bala cynwyd", "wynnewood", "narberth", "merion",
            "jenkintown", "abington", "willow grove", "ambler", "lansdale",
            "doylestown", "newtown", "yardley", "blue bell", "plymouth meeting",
            # New Jersey suburbs
            "camden", "cherry hill", "haddonfield", "moorestown", "mount laurel",
            "marlton", "voorhees", "collingswood", "haddon township",
            # Delaware
            "wilmington", "newark", "dover",
        ],
        zip_prefixes=["190", "191", "192", "193", "194", "195", "196",
                      "080", "081", "082", "083", "084", "085", "086",
                      "197", "198", "199"],
        search_locations=["Philadelphia, PA", "King of Prussia, PA", "Cherry Hill, NJ", "Wilmington, DE", "West Chester, PA"],
        state_abbrev="PA",
        non_metro_cities=["pittsburgh", "harrisburg", "allentown", "new york", "baltimore"],
    ),

    # -------------------------------------------------------------------------
    # PHOENIX
    # -------------------------------------------------------------------------
    "phoenix": MetroConfig(
        key="phoenix",
        display_name="Greater Phoenix Area",
        area_cities=[
            # Core Phoenix
            "phoenix", "downtown phoenix", "central phoenix", "arcadia",
            "biltmore", "north phoenix", "south phoenix", "west phoenix",
            "ahwatukee", "laveen", "maryvale", "encanto",
            # East Valley
            "scottsdale", "tempe", "mesa", "chandler", "gilbert", "queen creek",
            "apache junction", "gold canyon", "fountain hills", "paradise valley",
            # West Valley
            "glendale", "peoria", "surprise", "goodyear", "avondale", "buckeye",
            "litchfield park", "tolleson", "youngtown", "el mirage", "sun city",
            "sun city west", "wickenburg",
            # North Valley
            "cave creek", "carefree", "anthem", "new river",
        ],
        zip_prefixes=["850", "851", "852", "853", "854", "855", "856", "857", "858", "859",
                      "852", "853"],
        search_locations=["Phoenix, AZ", "Scottsdale, AZ", "Tempe, AZ", "Mesa, AZ", "Chandler, AZ"],
        state_abbrev="AZ",
        non_metro_cities=["tucson", "flagstaff", "prescott", "yuma", "sedona"],
    ),

    # -------------------------------------------------------------------------
    # SAN DIEGO
    # -------------------------------------------------------------------------
    "san_diego": MetroConfig(
        key="san_diego",
        display_name="Greater San Diego Area",
        area_cities=[
            # Core San Diego
            "san diego", "downtown san diego", "gaslamp", "little italy",
            "hillcrest", "north park", "south park", "normal heights",
            "university heights", "mission hills", "bankers hill", "balboa park",
            "old town", "mission valley", "fashion valley", "kearny mesa",
            # Beach communities
            "la jolla", "pacific beach", "mission beach", "ocean beach",
            "point loma", "coronado", "imperial beach",
            # North County coastal
            "del mar", "solana beach", "encinitas", "carlsbad", "oceanside",
            # North County inland
            "escondido", "san marcos", "vista", "poway", "rancho bernardo",
            "carmel mountain", "scripps ranch", "mira mesa", "rancho penasquitos",
            # East County
            "la mesa", "el cajon", "santee", "lakeside", "alpine",
            "spring valley", "lemon grove", "casa de oro",
            # South Bay
            "chula vista", "national city", "san ysidro", "otay mesa",
        ],
        zip_prefixes=["919", "920", "921", "922"],
        search_locations=["San Diego, CA", "Carlsbad, CA", "La Jolla, CA", "Chula Vista, CA", "Escondido, CA"],
        state_abbrev="CA",
        non_metro_cities=["los angeles", "san francisco", "oakland", "sacramento", "fresno"],
    ),

    # -------------------------------------------------------------------------
    # SEATTLE
    # -------------------------------------------------------------------------
    "seattle": MetroConfig(
        key="seattle",
        display_name="Greater Seattle Area",
        area_cities=[
            # Core Seattle
            "seattle", "downtown seattle", "capitol hill", "queen anne",
            "ballard", "fremont", "wallingford", "university district",
            "green lake", "northgate", "columbia city", "beacon hill",
            "west seattle", "south lake union", "belltown", "pioneer square",
            "first hill", "central district", "madison park", "montlake",
            "magnolia", "interbay", "rainier valley", "georgetown", "sodo",
            # Eastside
            "bellevue", "kirkland", "redmond", "bothell", "woodinville",
            "sammamish", "issaquah", "mercer island", "newcastle", "renton",
            "factoria", "crossroads", "downtown bellevue",
            # North
            "shoreline", "lake forest park", "kenmore", "mountlake terrace",
            "lynnwood", "edmonds", "mukilteo", "everett", "marysville",
            # South
            "tukwila", "seatac", "burien", "des moines", "federal way",
            "auburn", "kent", "covington", "maple valley", "black diamond",
            # Tacoma area
            "tacoma", "lakewood", "university place", "puyallup", "bonney lake",
            "gig harbor", "olympia", "lacey", "tumwater",
        ],
        zip_prefixes=["980", "981", "982", "983", "984", "985", "986", "990", "991", "992",
                      "983", "984"],
        search_locations=["Seattle, WA", "Bellevue, WA", "Tacoma, WA", "Redmond, WA", "Kirkland, WA"],
        state_abbrev="WA",
        non_metro_cities=["portland", "spokane", "vancouver"],
    ),
}


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_metro_config(location: str) -> MetroConfig:
    """
    Get metro config by display name or key.

    Args:
        location: Either display name (e.g., "Greater Los Angeles Area")
                  or key (e.g., "los_angeles")

    Returns:
        MetroConfig for the specified location

    Raises:
        ValueError if location not found
    """
    location_lower = location.lower().strip()

    # Try exact key match first
    if location_lower.replace(" ", "_") in METRO_CONFIGS:
        return METRO_CONFIGS[location_lower.replace(" ", "_")]

    # Try display name match
    for config in METRO_CONFIGS.values():
        if config.display_name.lower() == location_lower:
            return config

    # Try partial match on key
    for key, config in METRO_CONFIGS.items():
        if key in location_lower or location_lower in key:
            return config

    raise ValueError(f"Unknown metro area: {location}. Available metros: {list(METRO_CONFIGS.keys())}")


def get_all_metro_display_names() -> List[str]:
    """Get list of all display names for UI dropdowns (sorted alphabetically)"""
    return sorted([config.display_name for config in METRO_CONFIGS.values()])


def get_all_metro_keys() -> List[str]:
    """Get list of all metro keys"""
    return list(METRO_CONFIGS.keys())
