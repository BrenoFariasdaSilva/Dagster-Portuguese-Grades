from dagster import Definitions, load_assets_from_modules

# If you move the assets.py file to a different directory than the project root, you will need to update the import statement below.
# I moved the assets.py file from the portuguese_grades/ to the portuguese_grades/assets directory
# With that in mind, i renamed the assets.py file to __init__.py
# It allows me to import the assets from the assets directory without having to specify the file name 
# So the import went from "from assets.assets import assets" to "from . import assets"
# It makes the code more readable and easier to maintain

from . import assets

# Load the assets from the assets/__init__.py file
all_assets = load_assets_from_modules(modules=[assets])

# The Definitions object is used to define the assets that will be used in the project
# The assets are defined in the assets/__init__.py file
# The assets are loaded from the assets/__init__.py file and stored in the all_assets variable
# Definitions may include: assets, schedules, jobs, resources, executors and loggers.
# "Repositories" is a collection of definitions, but that name is deprecated and was replaced by "Definitions"
defs = Definitions(
    assets=all_assets,
)
