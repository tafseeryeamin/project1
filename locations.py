
BANGLADESH_DIVISIONS = [
    "Dhaka",
    "Chittagong",
    "Rajshahi",
    "Khulna",
    "Barisal",
    "Sylhet",
    "Rangpur",
    "Mymensingh"
]

# Districts organized by division
BANGLADESH_DISTRICTS = {
    "Dhaka": [
        "Dhaka", "Gazipur", "Narsingdi", "Manikganj", "Munshiganj",
        "Narayanganj", "Tangail", "Kishoreganj", "Madaripur",
        "Rajbari", "Gopalganj", "Faridpur", "Shariatpur"
    ],
    "Chittagong": [
        "Chittagong", "Cox's Bazar", "Rangamati", "Bandarban",
        "Khagrachari", "Noakhali", "Feni", "Lakshmipur",
        "Comilla", "Brahmanbaria", "Chandpur"
    ],
    "Rajshahi": [
        "Rajshahi", "Natore", "Naogaon", "Nawabganj", "Pabna",
        "Sirajganj", "Bogra", "Joypurhat"
    ],
    "Khulna": [
        "Khulna", "Bagerhat", "Satkhira", "Jessore", "Magura",
        "Jhenaidah", "Narail", "Kushtia", "Chuadanga", "Meherpur"
    ],
    "Barisal": [
        "Barisal", "Bhola", "Patuakhali", "Pirojpur", "Jhalokati", "Barguna"
    ],
    "Sylhet": [
        "Sylhet", "Moulvibazar", "Habiganj", "Sunamganj"
    ],
    "Rangpur": [
        "Rangpur", "Gaibandha", "Nilphamari", "Kurigram",
        "Lalmonirhat", "Dinajpur", "Thakurgaon", "Panchagarh"
    ],
    "Mymensingh": [
        "Mymensingh", "Jamalpur", "Sherpur", "Netrokona"
    ]
}

# Flatten the districts list for easier access
ALL_DISTRICTS = [district for districts in BANGLADESH_DISTRICTS.values() for district in districts]

# Function to get division for a district
def get_division_for_district(district):
    """Return the division name for a given district."""
    for division, districts in BANGLADESH_DISTRICTS.items():
        if district in districts:
            return division
    return None