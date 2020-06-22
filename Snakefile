PYTHON = "PYTHONPATH=./ python"
PANDOC = "pandoc --filter pantable --filter pandoc-fignos --filter pandoc-tablenos --filter pandoc-citeproc"
PYTHON_SCRIPT = "PYTHONPATH=./ python {input} {output}"
PYTHON_SCRIPT_WITH_CONFIG = PYTHON_SCRIPT + " {CONFIG_FILE}"

CONFIG_FILE = "config/default.yaml"

configfile: CONFIG_FILE
include: "rules/data-preprocessing.smk"
include: "rules/sonnendach.smk"
include: "rules/capacityfactors.smk"
include: "rules/potential.smk"
include: "rules/analysis.smk"
include: "rules/sync.smk"

localrules: all, clean

wildcard_constraints:
    layer = "({layer_list})".format(layer_list="|".join((f"({layer})" for layer in config["layers"]))),
    scenario = "({scenario_list})".format(scenario_list="|".join((f"({scenario})" for scenario in config["scenarios"])))

onstart:
    shell("mkdir -p build/logs")
onsuccess:
    if "email" in config.keys():
        shell("echo "" | mail -s 'possibility-for-electricity-autarky succeeded' {config[email]}")
onerror:
    if "email" in config.keys():
        shell("echo "" | mail -s 'possibility-for-electricity-autarky crashed' {config[email]}")


rule all:
    message: "Create zip file of EuroSPORES, regional, and national level land-eligibility data."
    input:
        csvs = expand("build/{res}/{subdir}.csv",
            subdir=["technical-potential/areas", "technical-potential-protected/areas", "technical-social-potential/areas","shared-coast", "demand", "population", "land-cover"],
            res=["national", "regional", "eurospores"]
        )
    output:
        "build/raw-potentials.zip"
    run:
        import zipfile
        import os
        os.chdir('build')
        zip_file = zipfile.ZipFile(output[0].lstrip('build/'), "w")
        for csv in input.csvs:
            zip_file.write(csv.lstrip('build/'))
        zip_file.close()
        os.chdir('..')


rule clean: # removes all generated results
    shell:
        """
        rm -r ./build/*
        echo "Data downloaded to data/ has not been cleaned."
        """