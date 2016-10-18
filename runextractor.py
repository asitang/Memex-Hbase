import subprocess



def extract(folderin,folderout,filenumber):
    output = subprocess.check_output(
        'CLASSPATH=".:/mnt/Training\ Tesseract/TesseractOCRConfig.properties" '
        +'java -Dtika.xml.content -jar /deploy/parser-indexer/parser-indexer-1.0-SNAPSHOT.jar postdump'
         +' -list '+folderin+'/'+filenumber+' -out '+folderout+'/'+filenumber,
        shell=True,
        )

    #print output here