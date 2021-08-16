#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Programming contest management system
# Copyright © 2017 Kiarash Golezardi <kiarashgolezardi@gmail.com>
# Copyright © 2017 Amir Keivan Mohtashami <akmohtashami97@gmail.com>
# Copyright © 2018 Stefano Maggiolo <s.maggiolo@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from future.builtins.disabled import *  # noqa
from future.builtins import *  # noqa

import io
import json
import logging
import os
import re
import subprocess

from datetime import timedelta

from cms.db import Task, Dataset, Manager, Testcase, Attachment, Statement

from .base_loader import TaskLoader


logger = logging.getLogger(__name__)


def make_timedelta(t):
    return timedelta(seconds=t)


class TpsTaskLoader(TaskLoader):
    """Loader for TPS exported tasks.
    """

    short_name = 'tps_task'
    description = 'TPS task format'

    @staticmethod
    def detect(path):
        """See docstring in class Loader.
        """
        return os.path.exists(os.path.join(path, "problem.json"))

    def task_has_changed(self):
        """See docstring in class Loader.
        """
        return True

    def _get_task_type_parameters(self, data, task_type, evaluation_param):
        parameters_str = data['task_type_params']
        if parameters_str is None or parameters_str == '':
            parameters_str = '{}'
        task_type_parameters = json.loads(parameters_str)
        par_prefix = 'task_type_parameters_%s' % task_type

        if task_type == 'Batch':
            par_compilation = '%s_compilation' % par_prefix
            par_input = '%s_io_0_inputfile' % par_prefix
            par_output = '%s_io_1_outputfile' % par_prefix
            par_user_managers = "%s_user_managers" % par_prefix
            if par_compilation not in task_type_parameters:
                task_type_parameters[par_compilation] = 'grader'
            if par_input not in task_type_parameters:
                task_type_parameters[par_input] = ''
            if par_output not in task_type_parameters:
                task_type_parameters[par_output] = ''
            if par_user_managers not in task_type_parameters:
                pas_grader = os.path.join(
                    self.path, 'grader', 'graderlib.pas')
                user_managers = ('['
                                 + '\\"grader.cpp\\"' + ', '
                                 + '\\"grader.java\\"' + ', '
                                 + '\\"graderlib.pas\\"'
                                 + ']')
                if not os.path.exists(pas_grader):
                    user_managers = '[\\"grader.%l\\"]'
                task_type_parameters[par_user_managers] = user_managers
            return [
                task_type_parameters[par_compilation],
                [task_type_parameters[par_input],
                 task_type_parameters[par_output]],
                evaluation_param,
            ]

        if task_type == 'Communication':
            par_processes = '%s_num_processes' % par_prefix
            if par_processes not in task_type_parameters:
                task_type_parameters[par_processes] = 1
            return [task_type_parameters[par_processes], "stub", "fifo_io"]

        if task_type == 'TwoSteps' or task_type == 'OutputOnly':
            return [evaluation_param]

        return []

    def get_task(self, get_statement=True):
        """See docstring in class Loader.
        """

        json_src = os.path.join(self.path, 'problem.json')
        if not os.path.exists(json_src):
            logger.critical('No task found.')
            raise IOError('No task found at path %s' % json_src)
        with io.open(json_src, 'rt', encoding='utf-8') as json_file:
            data = json.load(json_file)

        name = data['code']
        logger.info("Loading parameters for task %s.", name)

        args = {}

        args["name"] = name
        if 'problem_label' in data:
            args['title'] = '{}. {}'.format(data['problem_label'], data['title'])
        else:
            args['title'] = data['title']

        # Statements
        if get_statement:
            statements_dir = os.path.join(self.path, 'statement')
            if os.path.exists(statements_dir):
                statements = [
                    filename
                    for filename in os.listdir(statements_dir)
                    if filename[-4:] == ".pdf"]
                if len(statements) > 0:
                    args['statements'] = dict()
                    logger.info('Statements found')
                for statement in statements:
                    language = statement[:-4]
                    if language == "en_US":
                        args["primary_statements"] = ["en_US"]
                    digest = self.file_cacher.put_file_from_path(
                        os.path.join(statements_dir, statement),
                        "Statement for task %s (lang: %s)" %
                        (name, language))
                    args['statements'][language] = Statement(language, digest)

        # Attachments
        if get_statement:
            args["attachments"] = dict()
            attachments_path = os.path.join(self.path, name + '.zip')
            if os.path.exists(attachments_path):
                logger.info("Attachments found")
                filename = os.path.basename(attachments_path)
                digest = self.file_cacher.put_file_from_path(
                    attachments_path,
                    "Attachment %s for task %s" % (filename, name))
                args["attachments"][filename] = Attachment(filename, digest)

                '''
                for filename in os.listdir(attachments_dir):
                    digest = self.file_cacher.put_file_from_path(
                        os.path.join(attachments_dir, filename),
                        "Attachment %s for task %s" % (filename, name))
                    args["attachments"][filename] = Attachment(filename, digest)
                '''

        data["type"] = \
            data["type"][0].upper() + data["type"][1:]

        # Setting the submission format
        # Obtaining testcases' codename
        testcases_dir = os.path.join(self.path, 'tests')
        if not os.path.exists(testcases_dir):
            logger.warning('Testcase folder was not found')
            testcase_codenames = []
        else:
            testcase_codenames = sorted([
                filename[:-3]
                for filename in os.listdir(testcases_dir)
                if filename[-3:] == '.in'])
        if data["type"] == 'OutputOnly':
            args["submission_format"] = list()
            for codename in testcase_codenames:
                args["submission_format"].append("%s.out" % codename)
        elif data["type"] == 'Notice':
            args["submission_format"] = list()
        else:
            args["submission_format"] = ["%s.%%l" % name]

        # Task information
        if 'feedback_level' in data:
            args['feedback_level'] = data['feedback_level']

        # Tokens parameters
        # args['max_user_test_number'] = 10
        # args['min_user_test_interval'] = make_timedelta(60)

        args['token_mode'] = 'disabled'
        # args['token_max_number'] = 100
        # args['token_min_interval'] = make_timedelta(60)
        # args['token_gen_initial'] = 1
        # args['token_gen_number'] = 1
        # args['token_gen_interval'] = make_timedelta(1800)
        # args['token_gen_max'] = 2

        # Limits
        if 'max_submission_number' in data:
            args['max_submission_number'] = data['max_submission_number']
        if 'max_user_test_number' in data:
            args['max_user_test_number'] = data['max_user_test_number']
        if 'min_submission_interval' in data:
            if data['min_submission_interval'] is None:
                args['min_submission_interval'] = None
            else:
                args['min_submission_interval'] = make_timedelta(data['min_submission_interval'])
        if 'min_user_test_interval' in data:
            if data['min_user_test_interval'] is None:
                args['min_user_test_interval'] = None
            else:
                args['min_user_test_interval'] = make_timedelta(data['min_user_test_interval'])

        # Score options
        if 'score_precision' in data:
            args['score_precision'] = int(data['score_precision'])
        if 'score_mode' in data:
            args['score_mode'] = data['score_mode']

        task = Task(**args)

        ignore_datasets = data['ignore_datasets'] if 'ignore_datasets' in data else False

        if ignore_datasets:
            logger.info("Task parameters loaded.")
            logger.info("Dataset loading skipped.")

            return task

        args = dict()

        args["task"] = task
        args["description"] = "Default"
        args["autojudge"] = True

        if data['type'] != 'OutputOnly' \
                and data['type'] != 'Notice':
            args["time_limit"] = float(data['time_limit'])
            args["memory_limit"] = int(data['memory_limit'])

        args["managers"] = {}

        # Checker
        checker_dir = os.path.join(self.path, "checker")
        checker_src = os.path.join(checker_dir, "checker.cpp")

        ignore_checker = data['ignore_checker'] if 'ignore_checker' in data else False

        if ignore_checker:
            logger.info("Checker is ignored, using diff if necessary")
            evaluation_param = "diff"
        elif os.path.exists(checker_src):
            logger.info("Checker found, compiling")
            checker_exe = os.path.join(checker_dir, "checker")
            subprocess.call([
                "g++", "-x", "c++", "-std=gnu++14", "-O2", "-static", "-DCMS",
                "-o", checker_exe, checker_src
            ])
            digest = self.file_cacher.put_file_from_path(
                checker_exe,
                "Manager for task %s" % name)
            args["managers"]['checker'] = Manager("checker", digest)
            evaluation_param = "comparator"
        else:
            logger.info("Checker not found, using diff if necessary")
            evaluation_param = "diff"

        # Note that the original TPS worked with custom task type Batch2017
        # and Communication2017 instead of Batch and Communication.
        args["task_type"] = data['type']
        args["task_type_parameters"] = \
            self._get_task_type_parameters(
                data, data['type'], evaluation_param)

        # Graders
        graders_dir = os.path.join(self.path, 'grader')

        if data['type'] == 'TwoSteps':
            pas_manager = name + 'lib.pas'
            pas_manager_path = os.path.join(graders_dir, pas_manager)
            if not os.path.exists(pas_manager_path):
                digest = self.file_cacher.put_file_content(
                    ''.encode('utf-8'), 'Pascal manager for task %s' % name)
                args["managers"][pas_manager] = Manager(pas_manager, digest)

        def get_file_list(files_dir, prefix, except_files):
            rt = []
            for filename in os.listdir(files_dir):
                if not filename in except_files:
                    if os.path.isdir(os.path.join(files_dir, filename)):
                        rt += get_file_list(os.path.join(files_dir, filename), os.path.join(prefix, filename), except_files)
                    else:
                        rt.append(os.path.join(prefix, filename))
            return rt

        if not os.path.exists(graders_dir):
            logger.warning('Grader folder was not found')
            graders_list = []
        else:
            graders_list = get_file_list(graders_dir, '', {'manager.cpp'})
        for grader_name in graders_list:
            grader_src = os.path.join(graders_dir, grader_name)
            digest = self.file_cacher.put_file_from_path(
                grader_src,
                "Manager for task %s" % name)
            grader_name = os.path.basename(grader_name)
            if data['type'] == 'Communication' \
                    and os.path.splitext(grader_name)[0] == 'grader':
                grader_name = 'stub' + os.path.splitext(grader_name)[1]
            args["managers"][grader_name] = Manager(grader_name, digest)

        # Manager
        manager_src = os.path.join(graders_dir, 'manager.cpp')

        if os.path.exists(manager_src):
            logger.info("Manager found, compiling")
            manager_exe = os.path.join(graders_dir, "manager")
            subprocess.call([
                "g++", "-x", "c++", "-O2", "-static",
                "-o", manager_exe, manager_src
            ])
            digest = self.file_cacher.put_file_from_path(
                manager_exe,
                "Manager for task %s" % name)
            args["managers"]["manager"] = Manager("manager", digest)

        # Testcases
        args["testcases"] = {}

        for codename in testcase_codenames:
            infile = os.path.join(testcases_dir, "%s.in" % codename)
            outfile = os.path.join(testcases_dir, "%s.out" % codename)
            if not os.path.exists(outfile):
                logger.critical(
                    'Could not find the output file for testcase %s', codename)
                logger.critical('Aborting...')
                return

            input_digest = self.file_cacher.put_file_from_path(
                infile,
                "Input %s for task %s" % (codename, name))
            output_digest = self.file_cacher.put_file_from_path(
                outfile,
                "Output %s for task %s" % (codename, name))
            # if codename.split('-')[0] != '0':   # We don't need sample testcase in CMS
            testcase = Testcase(codename, True,
                                input_digest, output_digest)
            args["testcases"][codename] = testcase

        # Score Type
        subtasks_json_src = os.path.join(self.path, 'subtasks.json')
        if not os.path.exists(subtasks_json_src):
            number_tests = max(len(testcase_codenames), 1)
            args["score_type"] = "Sum"
            args["score_type_parameters"] = 100 / number_tests
        else:
            args["score_type"] = "GroupMin"
            parsed_data = []
            subtask_no = -1
            mapping_src = os.path.join(self.path, 'tests', 'mapping')
            with open(subtasks_json_src, 'rt', encoding='utf-8') as json_file:
                subtasks_data = json.load(json_file)

            use_mapping = os.path.exists(mapping_src)
            if use_mapping:
                mapping_data = {}
                for subtask in subtasks_data['subtasks']:
                    mapping_data[subtask] = []
                with open(mapping_src, 'rt', encoding='utf-8') as mapping_file:
                    for row in mapping_file:
                        row = row.strip().split(' ')
                        if len(row) == 2:
                            mapping_data[row[0]].append(row[1])

            add_optional_name = data['add_optional_name'] if 'add_optional_name' in data else False

            for subtask, subtask_data in subtasks_data['subtasks'].items():
                subtask_no += 1
                score = int(subtask_data["score"])
                if use_mapping:
                    codenames = sorted(list(set('^' + testcase.split('-')[0] + '\\-' for testcase in mapping_data[subtask])))
                    testcases = "|".join(codenames)
                    if testcases == '':
                        testcases = '|NO_TESTCASES_AVAILABLE'
                else:
                    testcases = subtask_data["regex"]
                optional_name = "Subtask %d" % subtask_no
                if subtask_no == 0 and score == 0:
                #     continue   # We don't need sample testcase in CMS
                    optional_name = "Samples"
                if add_optional_name:
                    parsed_data.append([score, testcases, optional_name])
                else:
                    parsed_data.append([score, testcases])
            args["score_type_parameters"] = parsed_data

        dataset = Dataset(**args)
        task.active_dataset = dataset

        logger.info("Task parameters loaded.")

        return task
