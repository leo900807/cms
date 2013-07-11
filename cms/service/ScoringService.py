#!/usr/bin/env python2
# -*- coding: utf-8 -*-

# Programming contest management system
# Copyright © 2010-2013 Giovanni Mascellani <mascellani@poisson.phc.unipi.it>
# Copyright © 2010-2012 Stefano Maggiolo <s.maggiolo@gmail.com>
# Copyright © 2010-2012 Matteo Boscariol <boscarim@hotmail.com>
# Copyright © 2013 Luca Wehrstedt <luca.wehrstedt@gmail.com>
# Copyright © 2013 Bernard Blackham <bernard@largestprime.net>
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

"""Scoring service. Its jobs is to handle everything is about
assigning scores and communicating them to the world.

In particular, it takes care of handling the internal way of keeping
the score (i.e., the ranking view) and send to the external ranking
services the scores, via http requests.

"""

# We enable monkey patching to make many libraries gevent-friendly
# (for instance, urllib3, used by requests)
import gevent.monkey
gevent.monkey.patch_all()

import simplejson as json
import base64

import gevent
from gevent import socket
from gevent import ssl

import requests
from urlparse import urljoin

from cms import config, default_argument_parser, logger
from cms.io import ServiceCoord
from cms.io.GeventLibrary import Service, rpc_method
from cms.db import ask_for_contest
from cms.db.SQLAlchemyAll import SessionGen, Submission, \
    Contest, Dataset, Task
from cms.grading.scoretypes import get_score_type
from cms.service import get_submission_results, get_datasets_to_judge
from cmscommon.DateTime import make_timestamp


class CannotSendError(Exception):
    pass


def encode_id(entity_id):
    """Encode the id using only A-Za-z0-9_.

    entity_id (string): the entity id to encode.
    return (string): encoded entity id.

    """
    encoded_id = ""
    for char in entity_id.encode('utf-8'):
        if char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ" \
               "abcdefghijklmnopqrstuvwxyz" \
               "0123456789":
            try:
                encoded_id += "_" + hex(ord(char))[-2:]
            except TypeError:
                logger.error("Entity %s cannot be send correctly, "
                             "sending anyway (this may cause errors)." %
                             entity_id)
        else:
            encoded_id += char
    return encoded_id


def safe_put_data(ranking, resource, data, operation):
    """Send some data to ranking using a PUT request.

    ranking (str): the URL of ranking server.
    resource (str): the relative path of the entity.
    data (dict): the data to json-encode and send.
    operation (str): a human-readable description of the operation
                     we're performing (to produce log messages).

    raise CannotSendError in case of communication errors.

    """
    try:
        url = urljoin(ranking, resource)
        res = requests.put(url, json.dumps(data),
                           verify=config.https_certfile)
    except Exception as error:
        logger.info("Error %r while %s." % (error, operation))
        raise CannotSendError
    if res.status_code != 200:
        logger.info("Status %s while %s." % (res.status_code, operation))
        raise CannotSendError


def send_submissions(ranking, submission_put_data):
    """Send a submission to the remote ranking.

    ranking (str): the URL of ranking server.
    submission_put_data (dict): dictionary to send to the ranking to
                                send the submission.

    raise CannotSendError in case of communication errors.

    """
    logger.info("Sending submissions to ranking %s." % ranking)

    safe_put_data(ranking, "submissions/", submission_put_data,
                  "sending submissions to ranking %s" % ranking)


def send_subchanges(ranking, subchange_put_data):
    """Send a change to a submission (token or score update).

    ranking (str): the URL of ranking server.
    subchange_put_data (dict): dictionary to send to the ranking to
                               update the submission.

    raise CannotSendError in case of communication errors.

    """
    logger.info("Sending subchanges to ranking %s." % ranking)

    safe_put_data(ranking, "subchanges/", subchange_put_data,
                  "sending subchanges to ranking %s" % ranking)


class ScoringService(Service):
    """Scoring service.

    """

    # How often we try to send data to remote rankings.
    CHECK_DISPATCH_TIME = 5.0

    # How often we look for submission not scored/tokened.
    JOBS_NOT_DONE_CHECK_TIME = 347.0

    def __init__(self, shard, contest_id):
        logger.initialize(ServiceCoord("ScoringService", shard))
        Service.__init__(self, shard, custom_logger=logger)

        self.contest_id = contest_id

        self.scorers = {}
        self._initialize_scorers()

        # If for some reason (SS switched off for a while, or broken
        # connection with ES), submissions have been left without
        # score, this is the set where you want to pur their ids. Note
        # that sets != {} if and only if there is an alive timeout for
        # the method "score_old_submission".
        #
        # submission_results_to_score and submission_results_scored
        # contain pairs of (submission_id, dataset_id).
        #
        # submissions_to_token and submission_tokened contain scalar
        # values of submission_id.
        self.submission_results_to_score = set()
        self.submissions_to_token = set()
        self.scoring_old_submission = False

        # We need to load every submission at start, but we don't want
        # to invalidate every score so that we can simply load the
        # score-less submissions. So we keep a set of submissions that
        # we analyzed (for scoring and for tokens).
        self.submission_results_scored = set()
        self.submissions_tokened = set()

        # Initialize queues for data we will send to ranking servers.
        self.initialize_queue = set()
        self.submission_queue = dict()
        self.subchange_queue = dict()

        for ranking in config.rankings:
            self.initialize_queue.add(ranking)

        gevent.spawn(self.dispath_operations_thread)

        self.add_timeout(self.search_jobs_not_done, None,
                         ScoringService.JOBS_NOT_DONE_CHECK_TIME,
                         immediately=True)

    def dispath_operations_thread(self):
        while True:
            self.dispatch_operations()
            gevent.sleep(ScoringService.CHECK_DISPATCH_TIME)

    def _initialize_scorers(self):
        """Initialize scorers, the ScoreType objects holding all
        submissions for a given task and deciding scores, and create
        an empty ranking view for the contest.

        """
        with SessionGen(commit=False) as session:
            contest = Contest.get_from_id(self.contest_id, session)

            for task in contest.tasks:
                for dataset in task.datasets:
                    try:
                        self.scorers[dataset.id] = \
                            get_score_type(dataset=dataset)
                    except Exception:
                        logger.critical(
                            "Cannot get score type for task %s(%d)" %
                            (task.name, dataset.id), exc_info=True)
                        self.exit()
            session.commit()

    @rpc_method
    def search_jobs_not_done(self):
        """Look in the database for submissions that have not been
        scored for no good reasons. Put the missing job in the queue.

        """
        # Do this only if we are not still loading old submission
        # (from the start of the service).
        if self.scoring_old_submission:
            return True

        with SessionGen(commit=False) as session:
            contest = Contest.get_from_id(self.contest_id, session)

            new_submission_results_to_score = set()
            new_submissions_to_token = set()

            for submission in contest.get_submissions():
                for dataset in get_datasets_to_judge(submission.task):
                    sr = submission.get_result(dataset)
                    sr_id = (submission.id, dataset.id)

                    if sr is not None and (sr.evaluated() or
                            sr.compilation_outcome == "fail") and \
                            sr_id not in self.submission_results_scored:
                        new_submission_results_to_score.add(sr_id)

                if submission.tokened() and \
                        submission.id not in self.submissions_tokened:
                    new_submissions_to_token.add(submission.id)

        new_s = len(new_submission_results_to_score)
        old_s = len(self.submission_results_to_score)
        new_t = len(new_submissions_to_token)
        old_t = len(self.submissions_to_token)
        logger.info("Submissions found to score/token: %d, %d." %
                    (new_s, new_t))
        if new_s + new_t > 0:
            self.submission_results_to_score |= new_submission_results_to_score
            self.submissions_to_token |= new_submissions_to_token
            if old_s + old_t == 0:
                self.add_timeout(self.score_old_submissions, None,
                                 0.5, immediately=False)

        # Run forever.
        return True

    def score_old_submissions(self):
        """The submissions in the submission_results_to_score set are
        evaluated submissions that we can assign a score to, and this
        method scores a bunch of these at a time. This method keeps
        getting called while the set is non-empty. (Exactly the same
        happens for the submissions to token.)

        Note: doing this way (instead of putting everything in the
        __init__) prevent freezing the service at the beginning in the
        case of many old submissions.

        """
        self.scoring_old_submission = True
        to_score = len(self.submission_results_to_score)
        to_token = len(self.submissions_to_token)
        to_score_now = to_score if to_score < 4 else 4
        to_token_now = to_token if to_token < 16 else 16
        logger.info("Old submission yet to score/token: %s/%s." %
                    (to_score, to_token))

        for unused_i in xrange(to_score_now):
            submission_id, dataset_id = self.submission_results_to_score.pop()
            self.new_evaluation(submission_id, dataset_id)
        if to_score - to_score_now > 0:
            return True

        for unused_i in xrange(to_token_now):
            submission_id = self.submissions_to_token.pop()
            self.submission_tokened(submission_id)
        if to_token - to_token_now > 0:
            return True

        logger.info("Finished loading old submissions.")
        self.scoring_old_submission = False
        return False

    def dispatch_operations(self):
        """Look at the operations still to do in the queue and tries
        to dispatch them

        """
        initialize_queue = self.initialize_queue
        submission_queue = self.submission_queue
        subchange_queue = self.subchange_queue
        self.initialize_queue = set()
        self.submission_queue = dict()
        self.subchange_queue = dict()
        pending = len(initialize_queue) + \
            len(submission_queue) + \
            len(subchange_queue)
        if pending > 0:
            logger.info("%s operations still pending." % pending)

        failed_rankings = set()

        new_initialize_queue = set()
        for ranking in initialize_queue:
            if ranking in failed_rankings:
                new_initialize_queue.add(ranking)
                continue
            try:
                self.initialize(ranking)
            except:
                logger.info("Ranking %s not connected or generic error." %
                            ranking)
                new_initialize_queue.add(ranking)
                failed_rankings.add(ranking)

        new_submission_queue = dict()
        for ranking, data in submission_queue.iteritems():
            if ranking in failed_rankings:
                new_submission_queue[ranking] = data
                continue
            try:
                send_submissions(ranking, data)
            except:
                logger.info("Ranking %s not connected or generic error." %
                            ranking)
                new_submission_queue[ranking] = data
                failed_rankings.add(ranking)

        new_subchange_queue = dict()
        for ranking, data in subchange_queue.iteritems():
            if ranking in failed_rankings:
                new_subchange_queue[ranking] = data
                continue
            try:
                send_subchanges(ranking, data)
            except:
                logger.info("Ranking %s not connected or generic error." %
                            ranking)
                new_subchange_queue[ranking] = data
                failed_rankings.add(ranking)

        self.initialize_queue |= new_initialize_queue
        for r in set(self.submission_queue) | set(new_submission_queue):
            new_submission_queue.setdefault(r, dict()). \
                update(self.submission_queue.get(r, dict()))
        self.submission_queue = new_submission_queue
        for r in set(self.subchange_queue) | set(new_subchange_queue):
            new_subchange_queue.setdefault(r, dict()). \
                update(self.subchange_queue.get(r, dict()))
        self.subchange_queue = new_subchange_queue

        # We want this to run forever.
        return True

    def initialize(self, ranking):
        """Send to the ranking all the data that are supposed to be
        sent before the contest: contest, users, tasks. No support for
        teams, flags and faces.

        ranking (str): the URL of ranking server.

        raise CannotSendError in case of communication errors.

        """
        logger.info("Initializing ranking %s." % ranking)

        with SessionGen(commit=False) as session:
            contest = Contest.get_from_id(self.contest_id, session)

            if contest is None:
                logger.error("Received request for unexistent contest "
                             "id %s." % self.contest_id)
                raise KeyError

            contest_name = contest.name
            contest_url = "contests/%s" % encode_id(contest_name)
            contest_data = {
                "name": contest.description,
                "begin": int(make_timestamp(contest.start)),
                "end": int(make_timestamp(contest.stop)),
                "score_precision": contest.score_precision}

            users = dict((encode_id(user.username),
                          {"f_name": user.first_name,
                           "l_name": user.last_name,
                           "team": None})
                         for user in contest.users
                         if not user.hidden)

            tasks = dict((encode_id(task.name),
                          {"name": task.title,
                           "contest": encode_id(contest.name),
                           "max_score": 100.0,
                           "score_precision": task.score_precision,
                           "extra_headers": [],
                           "order": task.num,
                           "short_name": task.name})
                         for task in contest.tasks)

        safe_put_data(ranking, contest_url, contest_data,
                      "sending contest %s to ranking %s" %
                      (contest_name, ranking))

        safe_put_data(ranking, "users/", users,
                      "sending users of contest %s to ranking %s" %
                      (contest_name, ranking))

        safe_put_data(ranking, "tasks/", tasks,
                      "sending tasks of contest %s to ranking %s" %
                      (contest_name, ranking))


    @rpc_method
    def reinitialize(self):
        """Inform the service that something in the data of the
        contest has changed (users, tasks, the contest itself) and we
        need to do it over again. This should be almost like
        restarting the service.

        """
        logger.info("Reinitializing rankings.")
        self.scorers = {}
        self._initialize_scorers()
        for ranking in config.rankings:
            self.initialize_queue.add(ranking)

    @rpc_method
    def new_evaluation(self, submission_id, dataset_id):
        """This RPC inform ScoringService that ES finished the work on
        a submission (either because it has been evaluated, or because
        the compilation failed).

        submission_id (int): the id of the submission that changed.
        dataset_id (int): the id of the dataset to use.

        """
        with SessionGen(commit=True) as session:
            submission = Submission.get_from_id(submission_id, session)

            if submission is None:
                logger.error("[new_evaluation] Couldn't find submission %d "
                             "in the database." % submission_id)
                raise ValueError

            dataset = Dataset.get_from_id(dataset_id, session)

            if dataset is None:
                logger.error("[new_evaluation] Couldn't find dataset %d "
                             "in the database." % dataset_id)
                raise ValueError

            submission_result = submission.get_result(dataset)

            if submission_result is None or not submission_result.compiled():
                logger.warning("[new_evaluation] Submission %d(%d) is not "
                               "compiled." %
                               (submission_id, dataset_id))
                return
            elif submission_result.compilation_outcome == "ok" and \
                    not submission_result.evaluated():
                logger.warning("[new_evaluation] Submission %d(%d) compiled "
                               "correctly but is not evaluated." %
                               (submission_id, dataset_id))
                return
            elif submission.user.hidden:
                logger.info("[new_evaluation] Submission %d not scored "
                            "because user is hidden." % submission_id)
                return

            # Assign score to the submission.
            scorer = self.scorers[dataset_id]
            scorer.add_submission(submission_id, submission.timestamp,
                                  submission.user.username,
                                  submission_result.evaluated(),
                                  dict((ev.codename,
                                        {"outcome": ev.outcome,
                                         "text": ev.text,
                                         "time": ev.execution_time,
                                         "memory": ev.memory_used})
                                       for ev in
                                       submission_result.evaluations),
                                  submission.tokened())

            # Mark submission as scored.
            self.submission_results_scored.add((submission_id, dataset_id))

            # Filling submission's score info in the db.
            submission_result.score = \
                scorer.pool[submission_id]["score"]
            submission_result.public_score = \
                scorer.pool[submission_id]["public_score"]

            # And details.
            submission_result.score_details = \
                scorer.pool[submission_id]["details"]
            submission_result.public_score_details = \
                scorer.pool[submission_id]["public_details"]
            submission_result.ranking_score_details = \
                scorer.pool[submission_id]["ranking_details"]

            try:
                ranking_score_details = json.loads(
                        submission_result.ranking_score_details)
            except (TypeError, ValueError):
                # It may be blank.
                ranking_score_details = None

            # If we are not a live dataset then we can bail out here,
            # and avoid updating RWS.
            if dataset is not submission.task.active_dataset:
                return

            # Data to send to remote rankings.
            submission_put_data = {
                "user": encode_id(submission.user.username),
                "task": encode_id(submission.task.name),
                "time": int(make_timestamp(submission.timestamp))}
            subchange_id = "%s%ss" % \
                (int(make_timestamp(submission.timestamp)),
                 submission_id)
            subchange_put_data = {
                "submission": encode_id(str(submission_id)),
                "time": int(make_timestamp(submission.timestamp)),
                # We're sending the unrounded score to RWS
                "score": submission_result.score}
            if ranking_score_details is not None:
                subchange_put_data["extra"] = ranking_score_details

        # TODO: ScoreRelative here does not work with remote
        # rankings (it does in the ranking view) because we
        # update only the user owning the submission.

        # Adding operations to the queue.
        for ranking in config.rankings:
            self.submission_queue.setdefault(
                ranking,
                dict())[encode_id(str(submission_id))] = \
                submission_put_data
            self.subchange_queue.setdefault(
                ranking,
                dict())[encode_id(subchange_id)] = \
                subchange_put_data

    @rpc_method
    def submission_tokened(self, submission_id):
        """This RPC inform ScoringService that the user has played the
        token on a submission.

        submission_id (int): the id of the submission that changed.
        timestamp (int): the time of the token.

        """
        with SessionGen(commit=False) as session:
            submission = Submission.get_from_id(submission_id, session)
            if submission is None:
                logger.error("[submission_tokened] Received token request for "
                             "unexistent submission id %s." % submission_id)
                raise KeyError
            elif submission.user.hidden:
                logger.info("[submission_tokened] Token for submission %d "
                            "not sent because user is hidden." % submission_id)
                return

            # Mark submission as tokened.
            self.submissions_tokened.add(submission_id)

            # Data to send to remote rankings.
            submission_put_data = {
                "user": encode_id(submission.user.username),
                "task": encode_id(submission.task.name),
                "time": int(make_timestamp(submission.timestamp))}
            subchange_id = "%s%st" % \
                (int(make_timestamp(submission.token.timestamp)),
                 submission_id)
            subchange_put_data = {
                "submission": encode_id(str(submission_id)),
                "time": int(make_timestamp(submission.token.timestamp)),
                "token": True}

        # Adding operations to the queue.
        for ranking in config.rankings:
            self.submission_queue.setdefault(
                ranking,
                dict())[encode_id(str(submission_id))] = \
                submission_put_data
            self.subchange_queue.setdefault(
                ranking,
                dict())[encode_id(subchange_id)] = \
                subchange_put_data

    @rpc_method
    def invalidate_submission(self,
                              submission_id=None,
                              dataset_id=None,
                              user_id=None,
                              task_id=None):
        """Request for invalidating some scores.

        Invalidate the scores of the SubmissionResults that:
        - belong to submission_id or, if None, to any submission of
          user_id and/or task_id or, if both None, to any submission
          of the contest this service is running for.
        - belong to dataset_id or, if None, to any dataset of task_id
          or, if None, to any dataset of any task of the contest this
          service is running for.

        submission_id (int): id of the submission to invalidate, or
                             None.
        dataset_id (int): id of the dataset to invalidate, or None.
        user_id (int): id of the user to invalidate, or None.
        task_id (int): id of the task to invalidate, or None.

        """
        logger.info("Invalidation request received.")

        # Validate arguments
        # TODO Check that all these objects belong to this contest.

        with SessionGen(commit=True) as session:
            submission_results = get_submission_results(
                # Give contest_id only if all others are None.
                self.contest_id
                    if {user_id, task_id, submission_id, dataset_id} == {None}
                    else None,
                user_id, task_id, submission_id, dataset_id, session)

            logger.info("Submission results to invalidate scores for: %d." %
                        len(submission_results))
            if len(submission_results) == 0:
                return

            new_submission_results_to_score = set()

            for submission_result in submission_results:
                # If the submission is not evaluated, it does not have
                # a score to invalidate, and, when evaluated,
                # ScoringService will be prompted to score it. So in
                # that case we do not have to do anything.
                if submission_result.evaluated():
                    submission_result.invalidate_score()
                    new_submission_results_to_score.add(
                        (submission_result.submission_id,
                         submission_result.dataset_id))

        old_s = len(self.submission_results_to_score)
        old_t = len(self.submissions_to_token)
        self.submission_results_to_score |= new_submission_results_to_score
        if old_s + old_t == 0:
            self.add_timeout(self.score_old_submissions, None,
                             0.5, immediately=False)

    @rpc_method
    def dataset_updated(self, task_id):
        """This function updates RWS with new data about a task. It should be
        called after the live dataset of a task is changed.

        task_id (int): id of the task whose dataset has changed.

        """
        with SessionGen(commit=False) as session:
            task = Task.get_from_id(task_id, session)
            dataset = task.active_dataset

            logger.info("Dataset update for task %d (dataset now is %d)." % (
                task.id, dataset.id))

            subchanges = []
            for submission in task.submissions:
                submission_result = submission.get_result(dataset)

                if submission_result is None:
                    # Not yet compiled, evaluated or scored.
                    score = None
                    ranking_score_details = None
                else:
                    score = submission_result.score
                    try:
                        ranking_score_details = json.loads(
                                submission_result.ranking_score_details)
                    except (TypeError, ValueError):
                        # It may be blank.
                        ranking_score_details = None

                # Data to send to remote rankings.
                subchange_id = "%s%ss" % \
                    (int(make_timestamp(submission.timestamp)),
                     submission.id)
                subchange_put_data = {
                    "submission": encode_id(str(submission.id)),
                    "time": int(make_timestamp(submission.timestamp))}
                if score is not None:
                    # We're sending the unrounded score to RWS
                    subchange_put_data["score"] = score
                if ranking_score_details is not None:
                    subchange_put_data["extra"] = ranking_score_details
                subchanges.append((subchange_id, subchange_put_data))

        # Adding operations to the queue.
        for ranking in config.rankings:
            for subchange_id, data in subchanges:
                self.subchange_queue.setdefault(
                    ranking,
                    dict())[encode_id(subchange_id)] = data


def main():
    """Parse arguments and launch service.

    """
    default_argument_parser("Score computer and relayer for CMS.",
                            ScoringService,
                            ask_contest=ask_for_contest).run()


if __name__ == "__main__":
    main()
