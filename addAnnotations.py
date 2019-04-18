#!/usr/bin/env python
# -*- coding: utf-8 -*-                                                                     
#                                                                                           
import sys
sys.path.insert(0, "/OMERO/OMERO.server/lib/python")

import re
import csv
import omero
from omero.gateway import BlitzGateway

def printAnnotation(conn, obj):
    print "Name: ", obj.getName()
    print "Description: ", obj.getDescription()
    for ann in obj.listAnnotations():
        if isinstance(ann, omero.gateway.MapAnnotationWrapper):
            for k, v in ann.getMapValueAsMap().iteritems(): 
                print ann.getId(), ":", k, "->", v
        if isinstance(ann, omero.gateway.TagAnnotationWrapper): #ann._obj.__class__.__name__ == "TagAnnotationI":
            print ann.getId(), ":", ann.getValue()
            obj_ids = [ann.getId()];
            conn.deleteObjects("Annotation", obj_ids, deleteAnns=True, deleteChildren=True);

# Deleting all tags
def deleteAllTagAnnotation(conn, obj):
    for ann in obj.listAnnotations():
        if isinstance(ann, omero.gateway.TagAnnotationWrapper):
            print "<<<DELETE>>> %s %s" % (ann.getName(), ann.getId())
            conn.deleteObject(ann.getId());
            #obj_ids = [ann.getId()];
            #conn.deleteObjects("Annotation", obj_ids, deleteAnns=True, deleteChildren=True);

# Deleting tag
def deleteTagAnnotation(conn, obj, name):
    for ann in obj.listAnnotations():
        if isinstance(ann, omero.gateway.TagAnnotationWrapper):
            print "<<<DELETE>>> %s" % (ann.getName())
            conn.deleteObject(ann.getId())

# Deleting map-annotation
def deleteAllMapAnnotation(conn, obj):
    obj_ids = []
    for ann in obj.listAnnotations():
        if isinstance(ann, omero.gateway.MapAnnotationWrapper):
            obj_ids.append(ann.getId());
            print "<<<DELETE MAP_ANNOTATION>>> %d" % (ann.getId())            
            conn.deleteObjects("Annotation", obj_ids, deleteAnns=True, deleteChildren=True);
    obj.save()

# Adding map-annotation
def addMapAnnotation(conn, obj, namespace, keyValueData):
    deleteAllMapAnnotation(conn, obj)

    if namespace == "Default":
        namespace = omero.constants.metadata.NSCLIENTMAPANNOTATION

    mapAnn = omero.gateway.MapAnnotationWrapper(conn)
    mapAnn.setNs(namespace)
    mapAnn.setValue(keyValueData)
    mapAnn.save()

    obj.linkAnnotation(mapAnn)
    obj.save()

    print "Object ID: %s, Name: %s, Anno. ID: %s, Namespace: %s" % (obj.getId(), obj.getName(), mapAnn.getId(), namespace)

# Adding new tag
def addTag(conn, obj, tag):
    tagAnn = omero.gateway.TagAnnotationWrapper(conn)
    tagAnn.setValue(tag)
    tagAnn.save()
    obj.linkAnnotation(tagAnn)

def addAnnotationByFile(conn, infile):
    label = ["Projectname", "Datasetname", "LocalID", "Method", # 0-3
             "License","Contactname", "Organization", "Department", "Laboratory", # 4-8
             "Contributers", "Description", "Organism", "PubMed", # 9-12
             "xScale", "yScale", "zScale", "xyzUnit", "tScale", "tUnit", "Download_URL"] # 13-19

    annotatedProject = []
    annotatedDataset = []
    annotatedImage = []

    csv_reader = csv.reader(open(infile, "r"), delimiter=",", quotechar='"')
    for row in csv_reader:
        # Metadata
        keyValueData_P = []
        keyValueData_D = []
        keyValueData_I = []

        length = len(row)

        for i in [4,5,6,7,8,9,10,11,12,19]:
            if i < length and row[i] != "":
                if i == 12 and re.match(r"\d\d\d\d\d\d\d\d", row[i]):
                    row[i] = 'https://www.ncbi.nlm.nih.gov/pubmed/' + row[i]

                keyValueData_D.append([label[i], row[i]])
                if i != 10 and i != 19: # Description and Download_URL
                    keyValueData_P.append([label[i], row[i]])
                    keyValueData_I.append([label[i], row[i]])

        project = None
        if row[0] != "":
            project = conn.getObject("Project", attributes={'name': row[0]})    # if name is unique
            if project.getId() not in annotatedProject:
                addMapAnnotation(conn, project, "Default", keyValueData_P)
                annotatedProject.append(project.getId())
                project.setDescription(omero.rtypes.rstring(row[3]))
                project.save()

        dataset = None
        if row[1] != "" and project is not None:
            for d in project.listChildren():
                if d.getName() == row[1]:
                    dataset = d
                    break

        if dataset == None:
            print "dataset not found: ", row[1], "in", row[1]

        if project is not None and dataset is not None:
            if dataset.getId() not in annotatedDataset:
                addMapAnnotation(conn, dataset, "Default", keyValueData_D)
                annotatedDataset.append(dataset.getId())
                if row[3] != "" and row[3] != "-":
                    dataset.setDescription(omero.rtypes.rstring(row[3]))
                    dataset.save()
                    #print "Dataset description: ", dataset.getDescription()

            for image in dataset.listChildren():
                if image.getId() not in annotatedImage:
                    addMapAnnotation(conn, image, "Default", keyValueData_I)
                    annotatedImage.append(image.getId())
                    if row[3] != "" and row[3] != "":
                        image.setDescription(omero.rtypes.rstring(row[3]))
                        image.save()

def searchObject(conn, obj_kind, obj_name):
    return conn.getObject(obj_kind, attributes={'name': obj_name})    # if name is unique


if __name__ == "__main__":
    HOST        = 'localhost'
    PORT        = '4064'
    USER        = 'public_user'
    GROUP       = 'group_of_public_user'
    PASSWORD    = 'password_of_public_user'

    ANN         = 'file'
    INPUTFILE   = "metadata_all.csv"

    argvs = sys.argv
    argc  = len(argvs)
    if argc == 1:
        print INPUTFILE
    elif argc == 2:
        INPUTFILE = argvs[1]
        print INPUTFILE
    elif argc >= 5:
        COM      = argvs[1]
        OBJ_KIND = argvs[2]
        OBJ_ID   = int(argvs[3])
        ANN      = argvs[4]
        if argc >= 6:
            KEY      = argvs[5]
        if argc >= 7:
            VAL  = argvs[6]
    else:
        sys.stderr.write("Argement error")
        sys.exit(1)

    try:
        conn = BlitzGateway(USER, PASSWORD, host=HOST, port=PORT)
        if not conn.connect():
            sys.stderr.write("Server connection error")
            sys.exit(1)

        for g in conn.getGroupsMemberOf():
            if g.getName() == GROUP:
                break

        conn.SERVICE_OPTS.setOmeroGroup(g.getId())

        if ANN == 'file':
            addAnnotationByFile(conn, INPUTFILE)
        else:
            obj = conn.getObject(OBJ_KIND, OBJ_ID)
            if obj is None:
                sys.stderr.write("Error: Object does not exist")
                sys.exit(1)
            print "Object = %s (ID: %s, Name: %s)" % (OBJ_KIND, obj.getId(), obj.getName())

            if COM == 'add':
                if ANN == 'des':
                    print "Add des (%s)" % (KEY)
                    obj.setDescription(omero.rtypes.rstring(KEY))
                elif ANN == 'tag':
                    print "Add tag (%s)" % (KEY)
                    addTag(conn, obj, KEY)
                elif ANN == 'map':
                    print "Add map-annotation (%s, %s)" % (KEY, VAL)
                    addMapAnnotation(conn, obj, "Metadata", [[KEY, VAL]])
                else:
                    sys.stderr.write("Error: Command does not exist")
                    sys.exit(1)
            if COM == 'del':
                if ANN == 'des':
                    print "Del des"
                    obj.setDescription(omero.rtypes.rstring(""))
                elif ANN == 'tag':
                    print "Del tag"
                    deleteAllTagAnnotation(conn, obj)
                elif ANN == 'map':
                    print "Del map"
                    deleteAllMapAnnotation(conn, obj)
                else:
                    sys.stderr.write("Error: Command does not exist")
                    sys.exit(1)

            printAnnotation(conn, obj)

    finally:
        #conn._closeSession()
        conn.seppuku()
