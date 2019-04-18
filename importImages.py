#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
import sys
sys.path.insert(0, "/OMERO/OMERO.server/lib/python")

import  os, csv, glob, time
import omero, omero.cli
from omero.gateway import BlitzGateway
from omero.rtypes import wrap
from omero.model import DatasetI, ProjectI

class AutoImporter:
    def __init__(self):
        self.HOST        = 'localhost'
        self.PORT        = '4064'
        self.USER        = 'public_user'
        self.GROUP       = 'group_of_public_user'
        self.PASSWORD    = 'password_of_public_user'

    def create_project(self, conn, project_name):
        print "Createing new Project: ", project_name
        p = ProjectI()
        p.name = wrap(project_name)
        return conn.getUpdateService().saveAndReturnObject(p)

    def create_dataset(self, conn, dataset_name):
        print "Creating new Dataset:", dataset_name
        d = DatasetI()
        d.name = wrap(dataset_name)
        return conn.getUpdateService().saveAndReturnObject(d)

    def link_dataset(self, conn, project, dataset):
        print "Linking Project and Dataset"
        link = omero.model.ProjectDatasetLinkI()
        link.parent = omero.model.ProjectI(project.getId(), False)
        link.child = omero.model.DatasetI(dataset.getId(), False)
        conn.getUpdateService().saveObject(link)

    def create_containers (self, project_name, dataset_name):
        """
        Creates containers with names provided if they don't exist already.
        Returns Project ID and Dataset ID.
        """
        conn = BlitzGateway(self.USER, self.PASSWORD, host=self.HOST, port=self.PORT)
        connected = conn.connect()
        if not connected:
            sys.stderr.write("Server connection error")
            sys.exit(1)

        for g in conn.getGroupsMemberOf():
            if g.getName() == self.GROUP:
                break

        conn.SERVICE_OPTS.setOmeroGroup(g.getId())
        params = omero.sys.Parameters()
                
        p = conn.getObject("Project", attributes={'name': project_name}, params=params)
        d = None
        if p is None:
            p = self.create_project(conn, project_name)
            d = self.create_dataset(conn, dataset_name)
            self.link_dataset(conn, p, d)
        else:
            print "Using existing Project", project_name
            for c in p.listChildren():
                if c.getName() == dataset_name:
                    d = c
                    break
                    
            if d is None:
                d = self.create_dataset(conn, dataset_name)
                self.link_dataset(conn, p, d)
            else:
                print "Using existing Dataset", dataset_name

        conn.seppuku()

        return p.getId(), d.getId()

    def do_import(self, project_name, dataset_name, ext, image_dir, image_num):
        image_files = []
        p_id = -1
        d_id = -1

        if os.path.isfile(image_dir): # target = file
            image_files.append(image_dir)
        elif os.path.isdir(image_dir): # target = dir
            search_command = '*' + ext
            image_files = glob.glob(os.path.join(image_dir, search_command))
        else:
            print "Cannot find file or dir:", image_dir
            return -1

        if len(image_files) != int(image_num):
            raise Exception("Number of images does not match: %s <> %s in %s" % (str(len(image_files)), str(int(image_num)), dataset_name))

        image_files.sort() # ソート
        for image_file in image_files:
            if os.path.isfile(image_file):
                cli = omero.cli.CLI()
                cli.loadplugins()
                cli.invoke(["login", "-s", self.HOST, "-p", self.PORT, "-u", self.USER, "-w", self.PASSWORD, "-C"], strict=True)
                cli.invoke(["sessions", "group", self.GROUP], strict=True)
                try:
                    p_id, d_id = self.create_containers(project_name, dataset_name)
                    title = os.path.basename(image_file) # OMEROで見えるファイル名
                    cli.invoke(["import", "-d", str(d_id), "-n", title, image_file], strict=True)
                    print p_id, d_id
                except:
                    print "Import failed for %s : %s" % (image_file, str(cli.rv))
                
                cli.close()
                
        return p_id, d_id

    def create_containers_from_file(self, input_file):
        print "INPUT:", input_file

        if not os.path.exists(input_file):
            sys.exit('ERROR: Input file %s was not found' % input_file)

        rf = csv.reader(open(input_file, 'r'), delimiter=',')

        for p_name, d_name, ext, image_dir, image_num in rf:
            # Skip for commented out data
            if p_name[0] == "#":
                continue
            self.create_containers(p_name, d_name)

    def do_import_from_file (self, input_file, output_file):
        print "INPUT:", input_file
        print "OUTPUT:", output_file

        if not os.path.exists(input_file):
            sys.exit('ERROR: Input file %s was not found' % input_file)

        rf = csv.reader(open(input_file, 'r'), delimiter=',')
        wf = csv.writer(open(output_file, 'w'), delimiter=',')

        for p_name, d_name, ext, image_dir, image_num in rf:
            # Skip for commented out data
            if p_name[0] == "#":
                continue

            s_time = time.time() # set start time
            p_id, d_id = self.do_import(p_name, d_name, ext, image_dir, image_num)
            e_time = time.time() # set end time
            
            tt = "%.1f" % ((e_time - s_time)/60.0)

            wf.writerow((p_name, d_name, ext, image_dir, image_num, p_id, d_id, tt))
            
if __name__ == "__main__": 
    INPUT_FILE  = 'test.csv'

    argvs = sys.argv
    argc  = len(argvs)
    if argc == 2:
        INPUT_FILE = argvs[1]

    fbase = os.path.splitext(os.path.basename(INPUT_FILE))[0]
    OUTPUT_FILE =  "%s_output.csv" % fbase

    ai = AutoImporter()
    ai.create_containers_from_file(INPUT_FILE)
    ai.do_import_from_file(INPUT_FILE, OUTPUT_FILE)

    print "INPUT: ", INPUT_FILE
    print "LOG: ", OUTPUT_FILE
