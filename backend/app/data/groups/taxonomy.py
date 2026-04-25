"""
96-group industry taxonomy for the Indian stock scanner.

Each group has:
  id      - stable slug used as primary_group_id in output + rank history
  name    - human-readable group name
  parent  - parent macro-sector; under-5 groups merge to parent for ranking only
  number  - canonical number in the taxonomy document

Parent sectors (14):
  financials, technology, telecom, capital_goods, transport, auto,
  materials, chemicals, energy, healthcare, consumer, retail_media,
  agri_food, real_estate
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GroupDef:
    number: int
    id: str
    name: str
    parent: str


TAXONOMY: list[GroupDef] = [
    # Financials (1-14)
    GroupDef(1,  "private_banks",                    "Private Banks",                           "financials"),
    GroupDef(2,  "psu_banks",                        "PSU Banks",                               "financials"),
    GroupDef(3,  "small_finance_banks",              "Small Finance Banks",                     "financials"),
    GroupDef(4,  "diversified_nbfcs",                "Diversified NBFCs",                       "financials"),
    GroupDef(5,  "vehicle_finance_nbfcs",            "Vehicle Finance NBFCs",                   "financials"),
    GroupDef(6,  "housing_finance",                  "Housing Finance",                         "financials"),
    GroupDef(7,  "gold_loan_nbfcs",                  "Gold Loan NBFCs",                         "financials"),
    GroupDef(8,  "microfinance",                     "Microfinance",                            "financials"),
    GroupDef(9,  "broking_wealth",                   "Broking & Wealth Management",             "financials"),
    GroupDef(10, "amc_exchanges_depositories",       "Asset Management, Exchanges & Depositories", "financials"),
    GroupDef(11, "life_insurance",                   "Life Insurance",                          "financials"),
    GroupDef(12, "general_insurance",                "General Insurance",                       "financials"),
    GroupDef(13, "fintech_payments",                 "Fintech & Payments",                      "financials"),
    GroupDef(14, "credit_rating_market_infra",       "Credit Rating & Market Infrastructure",   "financials"),

    # Technology / IT (15-21)
    GroupDef(15, "tier1_it_services",                "Tier-1 IT Services",                      "technology"),
    GroupDef(16, "midcap_it_services",               "Midcap IT Services",                      "technology"),
    GroupDef(17, "erd_engineering_tech",             "ER&D / Engineering Tech",                 "technology"),
    GroupDef(18, "software_saas",                    "Software Products / SaaS",                "technology"),
    GroupDef(19, "it_hardware_distribution",         "IT Hardware & Distribution",              "technology"),
    GroupDef(20, "ems_electronics_manufacturing",    "EMS / Electronics Manufacturing",         "technology"),
    GroupDef(21, "consumer_electronics_appliances",  "Consumer Electronics & Appliances",       "technology"),

    # Telecom (22-24)
    GroupDef(22, "telecom_services",                 "Telecom Services",                        "telecom"),
    GroupDef(23, "telecom_equipment_fiber",          "Telecom Equipment & Fiber",               "telecom"),
    GroupDef(24, "data_centers_digital_infra",       "Data Centers & Digital Infra",            "telecom"),

    # Capital Goods & Industrials (25-36)
    GroupDef(25, "electrical_equipment",             "Electrical Equipment",                    "capital_goods"),
    GroupDef(26, "wires_cables",                     "Wires & Cables",                          "capital_goods"),
    GroupDef(27, "transformers_td_equipment",        "Transformers & T&D Equipment",            "capital_goods"),
    GroupDef(28, "industrial_automation_drives",     "Industrial Automation & Drives",          "capital_goods"),
    GroupDef(29, "pumps_valves_flow_control",        "Pumps, Valves & Flow Control",            "capital_goods"),
    GroupDef(30, "bearings_industrial_components",   "Bearings & Industrial Components",        "capital_goods"),
    GroupDef(31, "aerospace_defense",                "Aerospace & Defense",                     "capital_goods"),
    GroupDef(32, "railways_epc_rolling_stock",       "Railways EPC & Rolling Stock",            "capital_goods"),
    GroupDef(33, "construction_infra_epc",           "Construction & Infra EPC",                "capital_goods"),
    GroupDef(34, "building_products",                "Building Products",                       "capital_goods"),
    GroupDef(35, "hvac_cooling_refrigeration",       "HVAC, Cooling & Refrigeration",           "capital_goods"),
    GroupDef(36, "industrial_gases",                 "Industrial Gases",                        "capital_goods"),

    # Transport & Logistics (37-40)
    GroupDef(37, "logistics_surface",                "Logistics - Surface",                     "transport"),
    GroupDef(38, "ports_port_services",              "Ports & Port Services",                   "transport"),
    GroupDef(39, "shipping",                         "Shipping",                                "transport"),
    GroupDef(40, "aviation_airports",                "Aviation & Airports",                     "transport"),

    # Auto & EV (41-48)
    GroupDef(41, "passenger_vehicles",               "Passenger Vehicles",                      "auto"),
    GroupDef(42, "commercial_vehicles",              "Commercial Vehicles",                     "auto"),
    GroupDef(43, "two_wheelers",                     "Two-Wheelers",                            "auto"),
    GroupDef(44, "auto_anc_powertrain",              "Auto Ancillaries - Powertrain",           "auto"),
    GroupDef(45, "auto_anc_tyres",                   "Auto Ancillaries - Tyres",                "auto"),
    GroupDef(46, "auto_anc_wiring_lighting",         "Auto Ancillaries - Wiring / Lighting",    "auto"),
    GroupDef(47, "auto_anc_forgings_castings",       "Auto Ancillaries - Forgings / Castings",  "auto"),
    GroupDef(48, "ev_components_charging",           "EV Components & Charging",                "auto"),

    # Materials (49-55)
    GroupDef(49, "cement",                           "Cement",                                  "materials"),
    GroupDef(50, "ceramics_tiles",                   "Ceramics & Tiles",                        "materials"),
    GroupDef(51, "pipes_plastic_bm",                 "Pipes & Plastic Building Materials",      "materials"),
    GroupDef(52, "steel",                            "Steel",                                   "materials"),
    GroupDef(53, "stainless_special_alloys",         "Stainless Steel & Special Alloys",        "materials"),
    GroupDef(54, "non_ferrous_metals",               "Non-Ferrous Metals",                      "materials"),
    GroupDef(55, "mining_minerals",                  "Mining & Minerals",                       "materials"),

    # Chemicals (56-60)
    GroupDef(56, "commodity_chemicals",              "Commodity Chemicals",                     "chemicals"),
    GroupDef(57, "specialty_chemicals",              "Specialty Chemicals",                     "chemicals"),
    GroupDef(58, "agrochemicals",                    "Agrochemicals",                           "chemicals"),
    GroupDef(59, "dyes_pigments_fluorochem",         "Dyes, Pigments & Fluorochemicals",        "chemicals"),
    GroupDef(60, "gas_distribution_equipment",       "Gas Distribution Equipment",              "chemicals"),

    # Energy (61-63)
    GroupDef(61, "oil_marketing_refining",           "Oil Marketing & Refining",                "energy"),
    GroupDef(62, "upstream_oil_gas",                 "Upstream Oil & Gas",                      "energy"),
    GroupDef(63, "city_gas_distribution",            "City Gas Distribution & Gas Infra",       "energy"),

    # Healthcare (64-70)
    GroupDef(64, "hospitals",                        "Hospitals",                               "healthcare"),
    GroupDef(65, "diagnostics",                      "Diagnostics",                             "healthcare"),
    GroupDef(66, "pharma_formulations",              "Pharma Formulations",                     "healthcare"),
    GroupDef(67, "pharma_apis",                      "Pharma APIs",                             "healthcare"),
    GroupDef(68, "cdmo_crams",                       "CDMO / CRAMS",                            "healthcare"),
    GroupDef(69, "biotech_vaccines",                 "Biotech & Vaccines",                      "healthcare"),
    GroupDef(70, "medical_devices",                  "Medical Devices",                         "healthcare"),

    # Consumer (71-83)
    GroupDef(71, "fmcg_staples",                     "FMCG Staples",                            "consumer"),
    GroupDef(72, "packaged_foods_bev",               "Packaged Foods & Beverages",              "consumer"),
    GroupDef(73, "qsr_restaurants",                  "QSR / Restaurants",                       "consumer"),
    GroupDef(74, "alcoholic_beverages",              "Alcoholic Beverages",                     "consumer"),
    GroupDef(75, "tobacco",                          "Tobacco",                                 "consumer"),
    GroupDef(76, "retail_grocery_value",             "Retail - Grocery / Value",                "retail_media"),
    GroupDef(77, "retail_fashion_lifestyle",         "Retail - Fashion / Lifestyle",            "retail_media"),
    GroupDef(78, "retail_electronics_omni",          "Retail - Electronics / Omni-channel",     "retail_media"),
    GroupDef(79, "hotels",                           "Hotels",                                  "retail_media"),
    GroupDef(80, "travel_ota",                       "Travel Services / Online Travel",         "retail_media"),
    GroupDef(81, "multiplex_exhibition",             "Multiplex / Exhibition",                  "retail_media"),
    GroupDef(82, "media_broadcasting",               "Media & Broadcasting",                    "retail_media"),
    GroupDef(83, "education_test_prep",              "Education / Test Prep",                   "retail_media"),

    # Textiles & Lifestyle (84-86)
    GroupDef(84, "apparel_textiles",                 "Apparel & Textiles",                      "consumer"),
    GroupDef(85, "home_furnishings",                 "Home Furnishings",                        "consumer"),
    GroupDef(86, "footwear_luggage",                 "Footwear & Luggage",                      "consumer"),

    # Agri & Food (87-91)
    GroupDef(87, "tractors_agri_machinery",          "Tractors & Agri Machinery",               "agri_food"),
    GroupDef(88, "sugar",                            "Sugar",                                   "agri_food"),
    GroupDef(89, "tea_coffee_plantations",           "Tea, Coffee & Plantations",               "agri_food"),
    GroupDef(90, "dairy_protein_foods",              "Dairy & Protein Foods",                   "agri_food"),
    GroupDef(91, "poultry_aqua_animal_health",       "Poultry, Aquaculture & Animal Health",    "agri_food"),

    # Real Estate (92-93)
    GroupDef(92, "real_estate_developers",           "Real Estate Developers",                  "real_estate"),
    GroupDef(93, "reits_commercial_re",              "REITs & Commercial Real Estate",          "real_estate"),

    # Power (94-96)
    GroupDef(94, "power_generation",                 "Power Generation",                        "energy"),
    GroupDef(95, "power_utilities_distribution",     "Power Utilities / Distribution",          "energy"),
    GroupDef(96, "renewable_energy",                 "Renewable Energy / Solar / Wind",         "energy"),
]


GROUPS_BY_ID: dict[str, GroupDef] = {g.id: g for g in TAXONOMY}
GROUPS_BY_NUMBER: dict[int, GroupDef] = {g.number: g for g in TAXONOMY}


PARENT_LABEL: dict[str, str] = {
    "financials": "Financials",
    "technology": "Technology / IT",
    "telecom": "Telecom",
    "capital_goods": "Capital Goods & Industrials",
    "transport": "Transport & Logistics",
    "auto": "Auto & EV",
    "materials": "Materials",
    "chemicals": "Chemicals",
    "energy": "Energy & Utilities",
    "healthcare": "Healthcare",
    "consumer": "Consumer",
    "retail_media": "Retail, Media & Services",
    "agri_food": "Agri & Food",
    "real_estate": "Real Estate",
}


def parent_bucket_id(parent: str) -> str:
    return f"__parent__{parent}"


def parent_bucket_name(parent: str) -> str:
    return f"{PARENT_LABEL.get(parent, parent.title())} (Parent bucket)"
