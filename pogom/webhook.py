#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import requests
import multiprocessing as mp
from datetime import datetime
from requests_futures.sessions import FuturesSession
import threading
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

log = logging.getLogger(__name__)

# How low do we want the queue size to stay?
wh_warning_threshold = 100
# How long can it be over the threshold, in seconds?
# Default: 5 seconds per 100 in threshold.
wh_threshold_lifetime = int(5 * (wh_warning_threshold / 100.0))
wh_lock = threading.Lock()


# TODO: Add logging.
class WebhookConsumer(mp.Process):

    def __init__(self, kill_event, task_queue, wh_session, webhooks,
                 wh_timeout, max_tasks):
        mp.Process.__init__(self)

        self.task_queue = task_queue
        self.kill_event = kill_event
        self.webhooks = webhooks
        self.wh_timeout = wh_timeout
        self.max_tasks = max_tasks
        self.running = True

        self.tasks_finished = 0  # Task counter.

        # Set up one session to use for all requests.
        # Requests to the same host will reuse the underlying TCP
        # connection, giving a performance increase.
        self.session = wh_session

    # Do work.
    def run(self):
        while self.running:
            # Get a new message.
            msg_type, message = self.task_queue.get()

            # Send it off.
            send_to_webhook(self.session,
                            msg_type,
                            message,
                            self.webhooks,
                            self.wh_timeout)

            # Mark item as done.
            self.task_queue.task_done()
            self.tasks_finished += 1

            # Renew if we've reached maximum tasks.
            if self.tasks_finished == self.max_tasks:
                self.running = False
                self.kill_event.set()  # Notify parent.

    # Tell the worker to stop.
    def stop(self):
        self.running = False


def send_to_webhook(session, message_type, message, webhooks, wh_timeout):
    if not webhooks:
        # What are you even doing here...
        log.warning('Called send_to_webhook() without webhooks.')
        return

    req_timeout = wh_timeout

    data = {
        'type': message_type,
        'message': message
    }

    for w in webhooks:
        try:
            session.post(w, json=data, timeout=(None, req_timeout),
                         background_callback=__wh_completed)
        except requests.exceptions.ReadTimeout:
            log.exception('Response timeout on webhook endpoint %s.', w)
        except requests.exceptions.RequestException as e:
            log.exception(repr(e))


def wh_updater(args, queue, key_cache, wh_session):
    wh_threshold_timer = datetime.now()
    wh_over_threshold = False

    # Set up one session to use for all requests.
    # Requests to the same host will reuse the underlying TCP
    # connection, giving a performance increase.
    session = wh_session

    # Use multiprocessing workers.
    num_consumers = args.wh_consumers
    max_tasks = args.wh_max_tasks
    mp_queue = mp.Queue()  # Multiprocessing task queue.
    kill_event = mp.Event()  # Triggers when a task needs to be renewed.

    # Start consumers.
    consumers = [WebhookConsumer(kill_event,
                                 mp_queue,
                                 session,
                                 args.webhooks,
                                 args.wh_timeout,
                                 max_tasks)
                 for i in xrange(num_consumers)]

    for w in consumers:
        w.start()

    # The forever loop.
    while True:
        try:
            # Loop the queue.
            whtype, message = queue.get()

            # If it's time to renew processes, renew.
            if kill_event.is_set():
                num_dead_processes = 0

                for w in consumers:
                    # Only renew dead ones.
                    if not w.is_alive():
                        # Remove dead process from consumers & increment.
                        consumers.remove(w)
                        num_dead_processes += 1

                # Add new processes.
                for i in xrange(num_dead_processes):
                    c = WebhookConsumer(mp_queue,
                                        session,
                                        args.webhooks,
                                        args.wh_timeout,
                                        max_tasks)
                    c.start()

                    # Add consumer to list.
                    consumers.append(c)

                # Reset flag.
                kill_event.clear()

            # Extract the proper identifier.
            ident_fields = {
                'pokestop': 'pokestop_id',
                'pokemon': 'encounter_id',
                'gym': 'gym_id'
            }
            ident = message.get(ident_fields.get(whtype), None)

            # cachetools in Python2.7 isn't thread safe, so we add a lock.
            with wh_lock:
                # Only send if identifier isn't already in cache.
                if ident is None:
                    # What it is isn't important, so let's just log and send
                    # as is.
                    log.debug(
                        'Sending webhook item of type: %s.', whtype)
                    mp_queue.put((whtype, message))
                elif ident not in key_cache:
                    key_cache[ident] = message
                    log.debug('Sending %s to webhook: %s.', whtype, ident)
                    mp_queue.put((whtype, message))
                else:
                    # Make sure to call key_cache[ident] in all branches so it
                    # updates the LFU usage count.

                    # If the object has changed in an important way, send new
                    # data to webhooks.
                    if __wh_object_changed(whtype, key_cache[ident], message):
                        key_cache[ident] = message
                        mp_queue.put((whtype, message))
                        log.debug('Sending updated %s to webhook: %s.',
                                  whtype, ident)
                    else:
                        log.debug('Not resending %s to webhook: %s.',
                                  whtype, ident)

            # Webhook queue moving too slow.
            if (not wh_over_threshold) and (
                    queue.qsize() > wh_warning_threshold):
                wh_over_threshold = True
                wh_threshold_timer = datetime.now()
            elif wh_over_threshold:
                if queue.qsize() < wh_warning_threshold:
                    wh_over_threshold = False
                else:
                    timediff = datetime.now() - wh_threshold_timer

                    if timediff.total_seconds() > wh_threshold_lifetime:
                        log.warning('Webhook queue has been > %d (@%d);'
                                    + ' for over %d seconds,'
                                    + ' try increasing --wh-concurrency'
                                    + ' or --wh-threads.',
                                    wh_warning_threshold,
                                    queue.qsize(),
                                    wh_threshold_lifetime)

            # Flag item as done.
            queue.task_done()
        except Exception as e:
            log.exception('Exception in wh_updater: %s.', repr(e))


# Helpers

# Background handler for completed webhook requests.
# Currently doesn't do anything.
def __wh_completed():
    pass


def get_webhook_requests_session(wh_retries, wh_backoff_factor,
                                 wh_concurrency):
    # Config / arg parser
    num_retries = wh_retries
    backoff_factor = wh_backoff_factor
    pool_size = wh_concurrency

    # Use requests & urllib3 to auto-retry.
    # If the backoff_factor is 0.1, then sleep() will sleep for [0.1s, 0.2s,
    # 0.4s, ...] between retries. It will also force a retry if the status
    # code returned is 500, 502, 503 or 504.
    session = FuturesSession(max_workers=pool_size)

    # If any regular response is generated, no retry is done. Without using
    # the status_forcelist, even a response with status 500 will not be
    # retried.
    retries = Retry(total=num_retries, backoff_factor=backoff_factor,
                    status_forcelist=[500, 502, 503, 504])

    # Mount handler on both HTTP & HTTPS.
    session.mount('http://', HTTPAdapter(max_retries=retries,
                                         pool_connections=pool_size,
                                         pool_maxsize=pool_size))
    session.mount('https://', HTTPAdapter(max_retries=retries,
                                          pool_connections=pool_size,
                                          pool_maxsize=pool_size))

    return session


def __get_key_fields(whtype):
    key_fields = {
        # lure_expiration is a UTC timestamp so it's good (Y).
        'pokestop': ['enabled', 'latitude',
                     'longitude', 'lure_expiration', 'active_fort_modifier'],
        'pokemon': ['spawnpoint_id', 'pokemon_id', 'latitude', 'longitude',
                    'disappear_time', 'move_1', 'move_2',
                    'individual_stamina', 'individual_defense',
                    'individual_attack'],
        'gym': ['team_id', 'guard_pokemon_id',
                'gym_points', 'enabled', 'latitude', 'longitude']
    }

    return key_fields.get(whtype, [])


# Determine if a webhook object has changed in any important way (and
# requires a resend).
def __wh_object_changed(whtype, old, new):
    # Only test for important fields: don't trust last_modified fields.
    fields = __get_key_fields(whtype)

    if not fields:
        log.debug('Received an object of unknown type %s.', whtype)
        return True

    return not __dict_fields_equal(fields, old, new)


# Determine if two dicts have equal values for all keys in a list.
def __dict_fields_equal(keys, a, b):
    for k in keys:
        if a.get(k) != b.get(k):
            return False

    return True
