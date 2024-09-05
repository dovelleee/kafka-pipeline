from .pipeline_stage import PipelineStage
from typing import List, Callable
from abc import ABC, abstractmethod
from multiprocessing import Process
from time import sleep

import docker

class KafkaPipeline(ABC):
    def __init__(self, pipeline_name: str,
                 stages: list):
        self._stages = stages
        self._name = pipeline_name

    @abstractmethod
    def start(self):
        pass

class MultiProcessPipeline(KafkaPipeline):
    def __init__(self, pipeline_name: str,
                 stage_factories: List[Callable],
                 delay_between_starts = 1):
        self._processess = []
        self._delay = delay_between_starts
        super().__init__(pipeline_name, stage_factories)

    def start(self):
        print(f'starting multiprocessing pipeline {self._name}', flush=True)
        for i, st in enumerate(self._stages):
            print(f'starting stage {i}', flush=True)
            p = Process(target=lambda s: s().start(), args=[st])
            p.start()
            self._processess.append(p)
            sleep(self._delay)
        
        # TODO: properly terminating child processes on keyboard interrupt
        for p in self._processess: p.join()

class DockerPipeline(KafkaPipeline):
    def __init__(self, pipeline_name: str, stages: List[str], docker_sock_path: str):
        self._docker = docker.DockerClient(base_url=docker_sock_path)
        super().__init__(pipeline_name, stages)

    def _wait_for_container_to_run(self, image_name, timeout=10):
        t = 0
        is_running = False
        while t < timeout or not is_running:
            for container in self._docker.containers.list():
                if container.image.id == image_name:
                    is_running = True
                    return is_running
            t += 1
        return is_running
        

    def start(self):
        for stage in self._stages:
            container = self._docker.containers.run(stage, detach=True)
            print(f'starting image: {stage} - \t\tid: {container.id}', flush=True)
            self._wait_for_container_to_run(stage, timeout=10)


